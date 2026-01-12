package main

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/joho/godotenv"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/robfig/cron/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/golang-jwt/jwt/v5"
	"golang.org/x/crypto/bcrypt"
)

/// ========== 데이터 모델 ==========

// 유저 프로필 (게임 데이터)
type UserProfile struct {
	UserID        string `json:"user_id" bson:"user_id"` // Unity PlayerID
	Nickname      string `json:"user_nickname" bson:"user_nickname"`
	CreatedAt     int64  `json:"created_at" bson:"created_at"`
	UserClass     string `json:"user_class,omitempty" bson:"user_class,omitempty"`
	UserLevel     int    `json:"user_level" bson:"user_level"`
	UserExp       int    `json:"user_exp" bson:"user_exp"`
	WeaponIconURL string `json:"weapon_icon_url,omitempty" bson:"weapon_icon_url,omitempty"`
	SkillIconURL  string `json:"skill_icon_url,omitempty" bson:"skill_icon_url,omitempty"`
}

// 홈페이지 로그인 계정(원본 계정)
type AuthUser struct {
	ID           primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	LoginID      string             `bson:"login_id" json:"login_id"`
	PasswordHash string             `bson:"password_hash" json:"-"`
	ExternalID   string             `bson:"external_id" json:"external_id"`
	UnityUserID  string             `bson:"unity_user_id,omitempty" json:"unity_user_id,omitempty"`
	CreatedAt    time.Time          `bson:"created_at" json:"created_at"`
}

type RegisterRequest struct {
	LoginID  string `json:"login_id"`
	Password string `json:"password"`
}
type LoginRequest struct {
	LoginID  string `json:"login_id"`
	Password string `json:"password"`
}

// Unity 토큰 교환 응답 (Token Exchange)
type TokenExchangeResponse struct {
	AccessToken string `json:"accessToken"`
}

// Custom ID sign-in 요청/응답
type CustomIDSignInRequest struct {
	ExternalID  string `json:"externalId"`
	SignInOnly  bool   `json:"signInOnly,omitempty"`
	AccessToken string `json:"accessToken,omitempty"` // 계정 링크할 때만 사용
}
type CustomIDSignInResponse struct {
	UserID       string `json:"userId"`
	IDToken      string `json:"idToken"`
	SessionToken string `json:"sessionToken"`
	ExpiresIn    int    `json:"expiresIn"`
}

// 유니티(클라)로 돌려줄 토큰 응답
type AuthTokenResponse struct {
	UserID       string `json:"userId"`
	AccessToken  string `json:"accessToken"` // = idToken
	SessionToken string `json:"sessionToken"`
	ExpiresIn    int    `json:"expiresIn"`
}

// 레벨/경험치 업데이트 요청 데이터
type UserStatsUpdateRequest struct {
	UserID    string `json:"user_id"`
	UserLevel int    `json:"user_level"`
	UserExp   int    `json:"user_exp"`
}

// Unity에서 받을 로그 데이터
type RawLog struct {
	ID          primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	UserID      string             `bson:"user_id" json:"user_id"`
	EventType   string             `bson:"event_type" json:"event_type"`
	Detail      interface{}        `bson:"detail" json:"detail"`
	MediaURL    string             `bson:"media_url,omitempty" json:"media_url,omitempty"`
	Timestamp   time.Time          `bson:"timestamp" json:"timestamp"`
	IsProcessed bool               `bson:"is_processed" json:"is_processed"`
}

// 생성된 이야기 데이터
type Story struct {
	ID        primitive.ObjectID `bson:"_id,omitempty" json:"story_id"`
	UserID    string             `bson:"user_id" json:"user_id"`
	Title     string             `bson:"title" json:"title"`
	Content   string             `bson:"content" json:"content"`
	CreatedAt time.Time          `bson:"created_at" json:"created_at"`
}

type HighlightDetail struct {
	Reason string `bson:"reason" json:"reason"`
	GifURL string `bson:"gif_url" json:"gif_url"`
}

// Gemini API 통신용 구조체
type GeminiRequest struct {
	Contents []GeminiContent `json:"contents"`
}
type GeminiContent struct {
	Parts []GeminiPart `json:"parts"`
}
type GeminiPart struct {
	Text string `json:"text"`
}
type GeminiResponse struct {
	Candidates []struct {
		Content struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"content"`
	} `json:"candidates"`
}

type InventoryDoc struct {
	UserID    string      `bson:"user_id"`
	Data      interface{} `bson:"data"` // JSON 데이터를 그대로 넣을 필드
	UpdatedAt time.Time   `bson:"updated_at"`
}

type WeaponDoc struct {
	UserID    string      `bson:"user_id"`
	Data      interface{} `bson:"data"` // JSON 전체를 저장
	UpdatedAt time.Time   `bson:"updated_at"`
}

// / ========== 전역 변수(DB) ==========
var (
	ctx = context.Background()

	userCollection      *mongo.Collection
	authCollection      *mongo.Collection
	logCollection       *mongo.Collection
	storyCollection     *mongo.Collection
	inventoryCollection *mongo.Collection
	weaponCollection    *mongo.Collection

	s3Client *s3.Client
)

/// ========== Unity 토큰 캐시(Stateless Token) ==========

type statelessTokenCache struct {
	mu    sync.Mutex
	token string
	exp   time.Time
}

var svcTokenCache statelessTokenCache

func getStatelessServiceToken() (string, error) {
	svcTokenCache.mu.Lock()
	defer svcTokenCache.mu.Unlock()

	// 아직 유효하면 재사용 (만료 2분 전이면 갱신)
	if svcTokenCache.token != "" && time.Until(svcTokenCache.exp) > 2*time.Minute {
		return svcTokenCache.token, nil
	}

	projectID := os.Getenv("UNITY_PROJECT_ID")
	envID := os.Getenv("UNITY_ENVIRONMENT_ID")
	keyID := os.Getenv("UNITY_SERVICE_ACCOUNT_KEY_ID")
	secret := os.Getenv("UNITY_SERVICE_ACCOUNT_SECRET")
	if projectID == "" || envID == "" || keyID == "" || secret == "" {
		return "", fmt.Errorf("UNITY_PROJECT_ID / UNITY_ENVIRONMENT_ID / UNITY_SERVICE_ACCOUNT_KEY_ID / UNITY_SERVICE_ACCOUNT_SECRET 환경변수 필요")
	}

	cred := base64.StdEncoding.EncodeToString([]byte(keyID + ":" + secret))

	url := fmt.Sprintf(
		"https://services.api.unity.com/auth/v1/token-exchange?projectId=%s&environmentId=%s",
		projectID, envID,
	)

	req, _ := http.NewRequest("POST", url, nil)
	req.Header.Set("Authorization", "Basic "+cred)
	req.Header.Set("Accept", "application/json")

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)

	// 200만 보지 말고 2xx 전체를 성공으로 처리
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("token-exchange 실패: %d / %s", resp.StatusCode, string(bodyBytes))
	}

	// (참고) 정상이라면 body에 accessToken이 와야 함. :contentReference[oaicite:2]{index=2}
	if len(bodyBytes) == 0 {
		return "", fmt.Errorf("token-exchange 성공(%d)인데 body가 비어있음. headers=%v", resp.StatusCode, resp.Header)
	}

	var te TokenExchangeResponse
	if err := json.Unmarshal(bodyBytes, &te); err != nil || te.AccessToken == "" {
		return "", fmt.Errorf("token-exchange 응답 파싱 실패: %s", string(bodyBytes))
	}

	// exp 파싱(서명검증 없이)해서 캐시 만료 잡기
	parser := jwt.NewParser()
	unverified, _, err := parser.ParseUnverified(te.AccessToken, jwt.MapClaims{})
	if err == nil {
		if claims, ok := unverified.Claims.(jwt.MapClaims); ok {
			if exp, err := claims.GetExpirationTime(); err == nil && exp != nil {
				svcTokenCache.exp = exp.Time
			} else {
				svcTokenCache.exp = time.Now().Add(55 * time.Minute)
			}
		}
	} else {
		svcTokenCache.exp = time.Now().Add(55 * time.Minute)
	}

	svcTokenCache.token = te.AccessToken
	log.Println("✔ Stateless service token refreshed. exp:", svcTokenCache.exp)
	return svcTokenCache.token, nil
}

/// ========== JWKS 캐시(플레이어 idToken 검증) ==========

type jwksCache struct {
	mu        sync.RWMutex
	set       jwk.Set
	fetchedAt time.Time
}

var unityJWKS jwksCache

func getUnityJWKS() (jwk.Set, error) {
	unityJWKS.mu.RLock()
	if unityJWKS.set != nil && time.Since(unityJWKS.fetchedAt) < 8*time.Hour {
		defer unityJWKS.mu.RUnlock()
		return unityJWKS.set, nil
	}
	unityJWKS.mu.RUnlock()

	unityJWKS.mu.Lock()
	defer unityJWKS.mu.Unlock()

	// 더블체크
	if unityJWKS.set != nil && time.Since(unityJWKS.fetchedAt) < 8*time.Hour {
		return unityJWKS.set, nil
	}

	jwksURL := "https://player-auth.services.api.unity.com/.well-known/jwks.json"
	resp, err := http.Get(jwksURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("JWKS fetch 실패: %d / %s", resp.StatusCode, string(b))
	}

	set, err := jwk.Parse(b)
	if err != nil {
		return nil, err
	}

	unityJWKS.set = set
	unityJWKS.fetchedAt = time.Now()
	log.Println("✔ Unity JWKS refreshed")
	return set, nil
}

func verifyUnityIDToken(idToken string) (jwt.MapClaims, error) {
	projectID := os.Getenv("UNITY_PROJECT_ID")
	if projectID == "" {
		return nil, fmt.Errorf("UNITY_PROJECT_ID 환경변수 필요")
	}

	// kid 읽기
	parser := jwt.NewParser()
	unverified, _, err := parser.ParseUnverified(idToken, jwt.MapClaims{})
	if err != nil {
		return nil, fmt.Errorf("token parse(unverified) 실패: %w", err)
	}

	kid, _ := unverified.Header["kid"].(string)
	if kid == "" {
		return nil, fmt.Errorf("token header에 kid 없음")
	}

	set, err := getUnityJWKS()
	if err != nil {
		return nil, err
	}

	key, ok := set.LookupKeyID(kid)
	if !ok {
		// 키 로테이션 대비: JWKS 강제 갱신 후 재시도
		unityJWKS.mu.Lock()
		unityJWKS.set = nil
		unityJWKS.mu.Unlock()

		set, err = getUnityJWKS()
		if err != nil {
			return nil, err
		}
		key, ok = set.LookupKeyID(kid)
		if !ok {
			return nil, fmt.Errorf("kid에 해당하는 jwk 키를 찾지 못함: %s", kid)
		}
	}

	var pub rsa.PublicKey
	if err := key.Raw(&pub); err != nil {
		return nil, fmt.Errorf("jwk -> rsa public key 변환 실패: %w", err)
	}

	// 서명/표준 클레임 검증
	token, err := jwt.Parse(idToken, func(t *jwt.Token) (interface{}, error) {
		return &pub, nil
	},
		jwt.WithValidMethods([]string{"RS256"}),
		jwt.WithIssuer("https://player-auth.services.api.unity.com"),
	)
	if err != nil {
		return nil, fmt.Errorf("jwt verify 실패: %w", err)
	}

	claims, ok := token.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("claims 타입 오류")
	}

	// project_id 체크(권장)
	if v, ok := claims["project_id"].(string); ok {
		if v != projectID {
			return nil, fmt.Errorf("project_id 불일치: %s", v)
		}
	}

	return claims, nil
}

/// ========== Web 세션 JWT 발급 ===========

type WebClaims struct {
	AuthUserID string `json:"auth_user_id"`
	LoginID    string `json:"login_id"`
	jwt.RegisteredClaims
}

func webSecret() []byte {
	s := os.Getenv("WEB_JWT_SECRET")
	if s == "" {
		// 포트폴리오라도 빈 값이면 위험하니까 바로 죽이는 게 안전
		log.Fatal("WEB_JWT_SECRET is not set")
	}
	return []byte(s)
}

func signWebToken(authUserID, loginID string) (string, error) {
	claims := WebClaims{
		AuthUserID: authUserID,
		LoginID:    loginID,
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   authUserID,
			Issuer:    "my-go-server",
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(7 * 24 * time.Hour)), // 7일
		},
	}
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	return t.SignedString(webSecret())
}

func parseWebToken(tokenStr string) (*WebClaims, error) {
	claims := new(WebClaims)
	parser := jwt.NewParser(jwt.WithValidMethods([]string{"HS256"}), jwt.WithLeeway(30*time.Second))
	tok, err := parser.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (interface{}, error) {
		return webSecret(), nil
	})
	if err != nil || tok == nil || !tok.Valid {
		return nil, fmt.Errorf("invalid web token")
	}
	return claims, nil
}

// / ========== 개발자 우회 미들웨어 ===========
func RequireDevBypass(c *fiber.Ctx) error {
	if os.Getenv("DEV_BYPASS_ENABLED") != "true" {
		return c.Status(404).JSON(fiber.Map{"error": "not found"})
	}
	key := c.Get("X-Dev-Key")
	if key == "" || key != os.Getenv("DEV_BYPASS_KEY") {
		return c.Status(401).JSON(fiber.Map{"error": "dev key required"})
	}
	return c.Next()
}

/// ========== 웹 인증 미들웨어 ===========

func RequireWebAuth(c *fiber.Ctx) error {
	// 1) 쿠키 우선
	tokenStr := c.Cookies("sid")

	// 2) 없으면 Authorization: Bearer
	if tokenStr == "" {
		auth := c.Get("Authorization")
		if strings.HasPrefix(auth, "Bearer ") {
			tokenStr = strings.TrimPrefix(auth, "Bearer ")
		}
	}

	if tokenStr == "" {
		return c.Status(401).JSON(fiber.Map{"error": "missing web session"})
	}

	claims, err := parseWebToken(tokenStr)
	if err != nil {
		return c.Status(401).JSON(fiber.Map{"error": "invalid web session"})
	}

	c.Locals("auth_user_id", claims.AuthUserID)
	c.Locals("login_id", claims.LoginID)
	return c.Next()
}

func getAuthedAuthUserID(c *fiber.Ctx) string {
	v := c.Locals("auth_user_id")
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

/// ========== Fiber 미들웨어 ==========

func RequireUnityAuth(c *fiber.Ctx) error {
	auth := c.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") {
		return c.Status(401).JSON(fiber.Map{"error": "missing bearer token"})
	}

	idToken := strings.TrimPrefix(auth, "Bearer ")
	claims, err := verifyUnityIDToken(idToken)
	if err != nil {
		return c.Status(401).JSON(fiber.Map{"error": "invalid token", "detail": err.Error()})
	}

	sub, _ := claims["sub"].(string) // Unity userId/playerId
	if sub == "" {
		return c.Status(401).JSON(fiber.Map{"error": "token has no sub"})
	}

	c.Locals("unity_user_id", sub)
	return c.Next()
}

func getAuthedUserID(c *fiber.Ctx) string {
	v := c.Locals("unity_user_id")
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func mustMatchUserID(c *fiber.Ctx, userID string) error {
	if userID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "user_id is required"})
	}
	if getAuthedUserID(c) != userID {
		return c.Status(403).JSON(fiber.Map{"error": "user_id mismatch"})
	}
	return nil
}

/// ========== Unity Custom ID 토큰 발급 ==========

func issueUnityTokensForExternalID(externalID string) (*CustomIDSignInResponse, error) {
	projectID := os.Getenv("UNITY_PROJECT_ID")
	envName := os.Getenv("UNITY_ENVIRONMENT_NAME")
	if projectID == "" || envName == "" {
		return nil, fmt.Errorf("UNITY_PROJECT_ID / UNITY_ENVIRONMENT_NAME 환경변수 필요")
	}

	stateless, err := getStatelessServiceToken()
	if err != nil {
		return nil, err
	}

	reqBody := CustomIDSignInRequest{
		ExternalID: externalID,
		SignInOnly: false,
	}
	b, _ := json.Marshal(reqBody)

	url := fmt.Sprintf("https://player-auth.services.api.unity.com/v1/projects/%s/authentication/server/custom-id", projectID)
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+stateless)
	req.Header.Set("UnityEnvironment", envName)

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("custom-id sign-in 실패: %d / %s", resp.StatusCode, string(bodyBytes))
	}

	var out CustomIDSignInResponse
	if err := json.Unmarshal(bodyBytes, &out); err != nil {
		return nil, fmt.Errorf("custom-id 응답 파싱 실패: %w", err)
	}
	if out.IDToken == "" || out.SessionToken == "" || out.UserID == "" {
		return nil, fmt.Errorf("custom-id 응답 필드 누락: %s", string(bodyBytes))
	}
	return &out, nil
}

/// ========== 웹 로그인 ===========

func WebLogin(c *fiber.Ctx) error {
	var req LoginRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
	}
	req.LoginID = strings.TrimSpace(req.LoginID)
	if req.LoginID == "" || req.Password == "" {
		return c.Status(400).JSON(fiber.Map{"error": "login_id and password are required"})
	}

	var user AuthUser
	err := authCollection.FindOne(ctx, bson.M{"login_id": req.LoginID}).Decode(&user)
	if err != nil {
		return c.Status(401).JSON(fiber.Map{"error": "invalid credentials"})
	}
	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		return c.Status(401).JSON(fiber.Map{"error": "invalid credentials"})
	}

	token, err := signWebToken(user.ID.Hex(), user.LoginID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "token issue failed"})
	}

	// 쿠키 세팅 (웹 전용)
	secure := os.Getenv("WEB_COOKIE_SECURE") == "true"
	domain := os.Getenv("WEB_COOKIE_DOMAIN")

	cookie := new(fiber.Cookie)
	cookie.Name = "sid"
	cookie.Value = token
	cookie.HTTPOnly = true
	cookie.Secure = secure
	cookie.SameSite = "Lax" // 포트폴리오 기본값으로 무난
	cookie.Path = "/"
	cookie.Expires = time.Now().Add(7 * 24 * time.Hour)
	if domain != "" {
		cookie.Domain = domain
	}
	c.Cookie(cookie)

	return c.JSON(fiber.Map{
		"status":   "success",
		"login_id": user.LoginID,
	})
}

func WebLogout(c *fiber.Ctx) error {
	c.Cookie(&fiber.Cookie{
		Name:     "sid",
		Value:    "",
		Path:     "/",
		Expires:  time.Unix(0, 0),
		HTTPOnly: true,
	})
	return c.JSON(fiber.Map{"status": "success"})
}

func WebMe(c *fiber.Ctx) error {
	authID := getAuthedAuthUserID(c)
	oid, err := primitive.ObjectIDFromHex(authID)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "bad auth_user_id"})
	}

	var user AuthUser
	if err := authCollection.FindOne(ctx, bson.M{"_id": oid}).Decode(&user); err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "user not found"})
	}

	return c.JSON(fiber.Map{
		"login_id":      user.LoginID,
		"external_id":   user.ExternalID,
		"unity_user_id": user.UnityUserID, // 없을 수도 있음
	})
}

func WebIssueUnityTokens(c *fiber.Ctx) error {
	authID := getAuthedAuthUserID(c)
	oid, err := primitive.ObjectIDFromHex(authID)
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "bad auth_user_id"})
	}

	var user AuthUser
	if err := authCollection.FindOne(ctx, bson.M{"_id": oid}).Decode(&user); err != nil {
		return c.Status(404).JSON(fiber.Map{"error": "user not found"})
	}

	tokens, err := issueUnityTokensForExternalID(user.ExternalID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "unity token issue failed", "detail": err.Error()})
	}

	// unity_user_id(sub)를 auth_users에 저장해두면 웹에서도 매핑이 쉬움
	unitySub := ""
	if claims, err := verifyUnityIDToken(tokens.IDToken); err == nil {
		if s, ok := claims["sub"].(string); ok {
			unitySub = s
		}
	}

	if unitySub != "" && unitySub != user.UnityUserID {
		_, _ = authCollection.UpdateOne(ctx,
			bson.M{"_id": oid},
			bson.M{"$set": bson.M{"unity_user_id": unitySub}},
		)
	}

	return c.JSON(AuthTokenResponse{
		UserID:       tokens.UserID,
		AccessToken:  tokens.IDToken,
		SessionToken: tokens.SessionToken,
		ExpiresIn:    tokens.ExpiresIn,
	})
}

/// ========== main ==========

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("⚠️WARNING: .env 파일을 찾을 수 없습니다. 환경 변수를 직접 확인합니다.")
	}

	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		log.Fatal("X MONGO_URI 환경 변수가 설정되지 않았습니다.")
	}

	// MongoDB 연결
	clientOptions := options.Client().ApplyURI(mongoURI)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connecting DB...")
	if err := client.Ping(ctx, nil); err != nil {
		log.Fatal("✖ MongoDB 연결 실패: ", err)
	}
	db := client.Database("GameDB")
	userCollection = db.Collection("users")
	authCollection = db.Collection("auth_users")
	logCollection = db.Collection("user_raw_logs")
	storyCollection = db.Collection("user_stories")
	inventoryCollection = db.Collection("user_inventories")
	weaponCollection = db.Collection("user_weapons")
	fmt.Println("✔ MongoDB Connected")

	initS3()

	// 스케줄러
	c := cron.New()
	c.AddFunc("@every 5m", GenerateStoriesBatchJob)
	c.Start()
	fmt.Print("⏱ Cron Scheduler Started\n\n")

	// Fiber
	app := fiber.New()
	app.Use(logger.New())

	webOrigin := os.Getenv("WEB_ORIGIN")
	if webOrigin == "" {
		webOrigin = "http://localhost:5173"
	}

	app.Use(cors.New(cors.Config{
		AllowOrigins:     webOrigin, // ★ "*" 금지 webOrigin 사용
		AllowHeaders:     "Origin, Content-Type, Accept, Authorization",
		AllowCredentials: true, // ★ 쿠키 사용
	}))

	/// ---- Auth (public) ----
	app.Post("/api/auth/register", Register)
	app.Post("/api/auth/login", Login)

	// Web session auth
	app.Post("/api/web/login", WebLogin)
	app.Post("/api/web/logout", WebLogout)
	app.Get("/api/web/me", RequireWebAuth, WebMe)
	app.Get("/api/web/unity-tokens", RequireWebAuth, WebIssueUnityTokens)

	/// ---- User APIs (protected) ----
	app.Get("/api/user/profile/:id", RequireUnityAuth, GetUserProfile)
	app.Post("/api/user/nickname", RequireUnityAuth, UpdateUserNickname)
	app.Post("/api/user/class", RequireUnityAuth, UpdateUserClass)
	app.Post("/api/user/stats", RequireUnityAuth, UpdateUserStats)

	app.Post("/api/log", RequireUnityAuth, IngestLog)
	app.Get("/api/stories/:user_id", RequireUnityAuth, GetUserStories)
	app.Post("/api/upload/image", RequireUnityAuth, UploadImageToS3)
	app.Post("/api/upload/highlight", RequireUnityAuth, UploadHighlightZip)

	// main() 함수 내부의 라우터 설정 부분에 추가
	app.Post("/api/user/weapon", RequireUnityAuth, UploadUserWeapon)      // 무기 데이터 업로드
	app.Get("/api/user/weapon/:user_id", RequireUnityAuth, GetUserWeapon) // 무기 데이터 다운로드

	app.Post("/api/user/inventory", RequireUnityAuth, UploadUserInventory)      // 인벤토리 업로드
	app.Get("/api/user/inventory/:user_id", RequireUnityAuth, GetUserInventory) // 인벤토리 다운로드

	fmt.Print("\n[Server Log]: Server Started\n")
	log.Fatal(app.Listen(":8000"))
}

/// ========== Auth handlers ==========

func Register(c *fiber.Ctx) error {
	var req RegisterRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
	}
	req.LoginID = strings.TrimSpace(req.LoginID)
	if req.LoginID == "" || req.Password == "" {
		return c.Status(400).JSON(fiber.Map{"error": "login_id and password are required"})
	}

	// 중복 체크
	cnt, err := authCollection.CountDocuments(ctx, bson.M{"login_id": req.LoginID})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "DB error"})
	}
	if cnt > 0 {
		return c.Status(409).JSON(fiber.Map{"error": "login_id already exists"})
	}

	hash, _ := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)

	oid := primitive.NewObjectID()
	user := AuthUser{
		ID:           oid,
		LoginID:      req.LoginID,
		PasswordHash: string(hash),
		ExternalID:   "acc_" + oid.Hex(),
		CreatedAt:    time.Now(),
	}

	if _, err := authCollection.InsertOne(ctx, user); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "register failed"})
	}

	return c.JSON(fiber.Map{"status": "success"})
}

func Login(c *fiber.Ctx) error {
	var req LoginRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
	}
	req.LoginID = strings.TrimSpace(req.LoginID)
	if req.LoginID == "" || req.Password == "" {
		return c.Status(400).JSON(fiber.Map{"error": "login_id and password are required"})
	}

	var user AuthUser
	err := authCollection.FindOne(ctx, bson.M{"login_id": req.LoginID}).Decode(&user)
	if err != nil {
		return c.Status(401).JSON(fiber.Map{"error": "invalid credentials"})
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		return c.Status(401).JSON(fiber.Map{"error": "invalid credentials"})
	}

	// Unity 토큰 발급
	tokens, err := issueUnityTokensForExternalID(user.ExternalID)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "unity token issue failed", "detail": err.Error()})
	}

	if claims, err := verifyUnityIDToken(tokens.IDToken); err == nil {
		if sub, ok := claims["sub"].(string); ok && sub != "" && sub != user.UnityUserID {
			_, _ = authCollection.UpdateOne(ctx,
				bson.M{"_id": user.ID},
				bson.M{"$set": bson.M{"unity_user_id": sub}},
			)
		}
	}

	return c.JSON(AuthTokenResponse{
		UserID:       tokens.UserID,
		AccessToken:  tokens.IDToken,
		SessionToken: tokens.SessionToken,
		ExpiresIn:    tokens.ExpiresIn,
	})
}

/// ========== User handlers ==========

// 유저 프로필 조회
func GetUserProfile(c *fiber.Ctx) error {
	userID := c.Params("id")
	if err := mustMatchUserID(c, userID); err != nil {
		return err
	}

	var user UserProfile
	err := userCollection.FindOne(ctx, bson.M{"user_id": userID}).Decode(&user)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return c.Status(404).JSON(fiber.Map{"message": "User not found"})
		}
		return c.Status(500).SendString("DB Error")
	}
	return c.JSON(user)
}

type NicknameRequest struct {
	UserID   string `json:"user_id"`
	Nickname string `json:"user_nickname"`
}

func UpdateUserNickname(c *fiber.Ctx) error {
	var req NicknameRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).SendString("Invalid JSON")
	}
	if err := mustMatchUserID(c, req.UserID); err != nil {
		return err
	}
	if strings.TrimSpace(req.Nickname) == "" {
		return c.Status(400).JSON(fiber.Map{"error": "user_nickname is required"})
	}

	filter := bson.M{"user_id": req.UserID}
	update := bson.M{
		"$set": bson.M{
			"user_nickname": req.Nickname,
		},
		"$setOnInsert": bson.M{
			"created_at": time.Now().Unix(),
			"user_level": 1,
			"user_exp":   0,
		},
	}
	opts := options.Update().SetUpsert(true)

	if _, err := userCollection.UpdateOne(ctx, filter, update, opts); err != nil {
		return c.Status(500).SendString("Save Failed")
	}
	return c.JSON(fiber.Map{"status": "success", "nickname": req.Nickname})
}

type ClassUpdateRequest struct {
	UserID    string `json:"user_id"`
	UserClass string `json:"user_class"`
}

func UpdateUserClass(c *fiber.Ctx) error {
	var req ClassUpdateRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
	}
	if err := mustMatchUserID(c, req.UserID); err != nil {
		return err
	}

	// user_class는 ""(None)도 허용
	filter := bson.M{"user_id": req.UserID}
	update := bson.M{"$set": bson.M{"user_class": req.UserClass}}

	result, err := userCollection.UpdateOne(ctx, filter, update)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "DB Update Failed"})
	}
	if result.MatchedCount == 0 {
		return c.Status(404).JSON(fiber.Map{"error": "User not found"})
	}
	return c.JSON(fiber.Map{"status": "success", "class": req.UserClass})
}

// 레벨 및 경험치 저장
func UpdateUserStats(c *fiber.Ctx) error {
	var req UserStatsUpdateRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
	}
	if err := mustMatchUserID(c, req.UserID); err != nil {
		return err
	}

	filter := bson.M{"user_id": req.UserID}
	update := bson.M{"$set": bson.M{"user_level": req.UserLevel, "user_exp": req.UserExp}}

	result, err := userCollection.UpdateOne(ctx, filter, update)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "Stats Update Failed"})
	}
	if result.MatchedCount == 0 {
		return c.Status(404).JSON(fiber.Map{"error": "User not found"})
	}
	return c.JSON(fiber.Map{"status": "success"})
}

// 로그 저장 (Unity -> Go)
func IngestLog(c *fiber.Ctx) error {
	var logData RawLog
	if err := c.BodyParser(&logData); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
	}
	if err := mustMatchUserID(c, logData.UserID); err != nil {
		return err
	}

	logData.Timestamp = time.Now()
	logData.IsProcessed = false

	_, err := logCollection.InsertOne(ctx, logData)
	if err != nil {
		return c.Status(500).SendString(err.Error())
	}
	return c.JSON(fiber.Map{"status": "saved"})
}

// 이야기 목록 조회 (Go -> Unity)
func GetUserStories(c *fiber.Ctx) error {
	userID := c.Params("user_id")
	if err := mustMatchUserID(c, userID); err != nil {
		return err
	}

	opts := options.Find().SetSort(bson.D{{Key: "created_at", Value: -1}})
	cursor, err := storyCollection.Find(ctx, bson.M{"user_id": userID}, opts)
	if err != nil {
		return c.Status(500).SendString(err.Error())
	}

	var stories []Story
	if err = cursor.All(ctx, &stories); err != nil {
		return c.Status(500).SendString(err.Error())
	}
	return c.JSON(fiber.Map{"stories": stories})
}

/// ========== S3 ==========

func initS3() {
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	region := os.Getenv("AWS_REGION")
	if accessKey == "" || secretKey == "" || region == "" {
		log.Println("⚠ AWS 환경변수 미설정: S3 기능은 비활성일 수 있습니다.")
		return
	}

	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		log.Println("⚠ AWS 설정 로드 실패:", err)
		return
	}
	s3Client = s3.NewFromConfig(cfg)
	fmt.Println("✔ AWS S3 Client Connected")
}

// 이미지 업로드
func UploadImageToS3(c *fiber.Ctx) error {
	if s3Client == nil {
		return c.Status(500).JSON(fiber.Map{"error": "S3 not configured"})
	}

	file, err := c.FormFile("image")
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "이미지 파일을 찾을 수 없습니다."})
	}

	src, err := file.Open()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "파일을 열 수 없습니다."})
	}
	defer src.Close()

	userID := c.FormValue("user_id")
	imageType := c.FormValue("image_type")
	if err := mustMatchUserID(c, userID); err != nil {
		return err
	}

	ext := filepath.Ext(file.Filename)
	if ext == "" {
		ext = ".jpg"
	}

	var s3Key string
	switch imageType {
	case "screenshot":
		timestamp := time.Now().Format("20060102_150405")
		s3Key = fmt.Sprintf("%s/screenshots/%s%s", userID, timestamp, ext)
	case "weapon":
		s3Key = fmt.Sprintf("%s/weapon_icon%s", userID, ext)
	case "skill":
		s3Key = fmt.Sprintf("%s/skill_icon%s", userID, ext)
	default:
		return c.Status(400).JSON(fiber.Map{"error": "invalid image_type"})
	}

	bucketName := os.Getenv("S3_BUCKET_NAME")
	region := os.Getenv("AWS_REGION")
	if bucketName == "" || region == "" {
		return c.Status(500).JSON(fiber.Map{"error": "S3_BUCKET_NAME/AWS_REGION not configured"})
	}

	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(s3Key),
		Body:        src,
		ContentType: aws.String("image/jpeg"),
	})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "S3 업로드 실패", "detail": err.Error()})
	}

	fileURL := fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", bucketName, region, s3Key)

	// 아이콘이면 프로필 URL 업데이트
	if imageType == "weapon" || imageType == "skill" {
		dbField := "weapon_icon_url"
		if imageType == "skill" {
			dbField = "skill_icon_url"
		}
		_, _ = userCollection.UpdateOne(ctx, bson.M{"user_id": userID}, bson.M{"$set": bson.M{dbField: fileURL}})
	}

	return c.JSON(fiber.Map{"status": "success", "type": imageType, "image_url": fileURL})
}

// 하이라이트 ZIP(GIF) 업로드
func UploadHighlightZip(c *fiber.Ctx) error {
	if s3Client == nil {
		return c.Status(500).JSON(fiber.Map{"error": "S3 not configured"})
	}

	// ✅ ffmpeg 설치 여부 먼저 확인 (에러 메시지 명확하게)
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		return c.Status(500).JSON(fiber.Map{
			"error":  "ffmpeg not found",
			"detail": "ffmpeg가 설치되어 있지 않거나 PATH에 없습니다.",
		})
	}

	userID := c.FormValue("user_id")
	if err := mustMatchUserID(c, userID); err != nil {
		return err
	}

	reason := strings.TrimSpace(c.FormValue("reason"))
	if reason == "" {
		reason = "highlight"
	}

	file, err := c.FormFile("zip")
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "zip file is required"})
	}

	// 용량 제한 (20MB)
	if file.Size > 20*1024*1024 {
		return c.Status(413).JSON(fiber.Map{"error": "zip too large"})
	}
	// 임시 작업 폴더
	workDir, err := os.MkdirTemp("", "hl_*")
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "temp dir failed"})
	}
	defer os.RemoveAll(workDir)

	zipPath := filepath.Join(workDir, "frames.zip")
	if err := c.SaveFile(file, zipPath); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "save zip failed", "detail": err.Error()})
	}

	framesDir := filepath.Join(workDir, "frames")
	if err := os.MkdirAll(framesDir, 0755); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "mkdir frames failed", "detail": err.Error()})
	}
	if err := unzipToSafe(zipPath, framesDir); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "unzip failed", "detail": err.Error()})
	}

	// ffmpeg로 GIF 생성
	palettePath := filepath.Join(workDir, "palette.png")
	gifPath := filepath.Join(workDir, "out.gif")

	// ※ Unity에서 frame_00001.jpg 형태로 만들었으니 여기 패턴과 일치해야 함.
	// fps/scale 값은 Unity 캡처 설정과 맞추는게 제일 깔끔.
	const fps = "12"
	const scale = "480:-1"

	cmd1 := exec.Command("ffmpeg",
		"-y",
		"-framerate", fps,
		"-i", filepath.Join(framesDir, "frame_%05d.jpg"),
		"-vf", fmt.Sprintf("fps=%s,scale=%s:flags=lanczos,palettegen", fps, scale),
		palettePath,
	)
	if out, err := cmd1.CombinedOutput(); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "ffmpeg palettegen failed", "detail": string(out)})
	}

	cmd2 := exec.Command("ffmpeg",
		"-y",
		"-framerate", fps,
		"-i", filepath.Join(framesDir, "frame_%05d.jpg"),
		"-i", palettePath,
		"-lavfi", fmt.Sprintf("fps=%s,scale=%s:flags=lanczos[x];[x][1:v]paletteuse", fps, scale),
		gifPath,
	)
	if out, err := cmd2.CombinedOutput(); err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "ffmpeg gif failed", "detail": string(out)})
	}

	// S3 업로드
	stamp := time.Now().Format("20060102_150405")
	s3Key := fmt.Sprintf("%s/highlights/%s.gif", userID, stamp)

	bucketName := os.Getenv("S3_BUCKET_NAME")
	region := os.Getenv("AWS_REGION")
	if bucketName == "" || region == "" {
		return c.Status(500).JSON(fiber.Map{"error": "S3_BUCKET_NAME/AWS_REGION not configured"})
	}

	f, err := os.Open(gifPath)
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "open gif failed", "detail": err.Error()})
	}
	defer f.Close()

	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(s3Key),
		Body:        f,
		ContentType: aws.String("image/gif"),
	})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "S3 upload failed", "detail": err.Error()})
	}

	gifURL := fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", bucketName, region, s3Key)

	// ✅ 여기서 로그를 “서버에서” 바로 저장
	logDoc := RawLog{
		UserID:      userID,
		EventType:   "highlight",
		Detail:      HighlightDetail{Reason: reason, GifURL: gifURL}, // 또는 bson.M{"reason": reason, "gif_url": gifURL}
		MediaURL:    gifURL,
		Timestamp:   time.Now(),
		IsProcessed: false,
	}

	ins, err := logCollection.InsertOne(ctx, logDoc)
	if err != nil {
		// (선택) 일관성 유지: DB 저장 실패 시 S3 파일 삭제 시도
		// IAM에 s3:DeleteObject 권한 필요
		_, _ = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(s3Key),
		})

		return c.Status(500).JSON(fiber.Map{
			"error":   "log insert failed",
			"detail":  err.Error(),
			"gif_url": gifURL, // 참고로 업로드는 성공했었음(삭제 실패 가능성도 있으니 정보 제공)
		})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"gif_url": gifURL,
		"log_id":  ins.InsertedID,
	})
}

func unzipToSafe(zipFile, dest string) error {
	r, err := zip.OpenReader(zipFile)
	if err != nil {
		return err
	}
	defer r.Close()

	base := filepath.Clean(dest) + string(os.PathSeparator)

	for _, f := range r.File {
		fp := filepath.Join(dest, f.Name)

		// zip slip 방지
		if !strings.HasPrefix(filepath.Clean(fp)+string(os.PathSeparator), base) &&
			filepath.Clean(fp) != filepath.Clean(dest) {
			return fmt.Errorf("illegal file path: %s", f.Name)
		}

		if f.FileInfo().IsDir() {
			if err := os.MkdirAll(fp, 0755); err != nil {
				return err
			}
			continue
		}

		if err := os.MkdirAll(filepath.Dir(fp), 0755); err != nil {
			return err
		}

		src, err := f.Open()
		if err != nil {
			return err
		}

		dst, err := os.Create(fp)
		if err != nil {
			src.Close()
			return err
		}

		_, copyErr := io.Copy(dst, src)
		dst.Close()
		src.Close()

		if copyErr != nil {
			return copyErr
		}
	}
	return nil
}

/// ========== AI 배치 ==========

func GenerateStoriesBatchJob() {
	fmt.Println("🚀 [Batch] 이야기 생성 작업 시작...")

	users, err := logCollection.Distinct(ctx, "user_id", bson.M{"is_processed": false})
	if err != nil {
		log.Println("Error finding users:", err)
		return
	}
	for _, u := range users {
		userID, _ := u.(string)
		if userID != "" {
			processUserLogs(userID)
		}
	}
}

// ... 기존 코드 하단에 추가 ...

/// ========== Weapon JSON Data Handlers ==========

func UploadUserWeapon(c *fiber.Ctx) error {
	// 1. 유저 인증 확인
	userID := getAuthedUserID(c)
	if userID == "" {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	// 2. 요청 바디(JSON) 파싱
	// Unity에서 보낸 JSON 데이터를 맵으로 받습니다.
	var bodyData map[string]interface{}
	if err := c.BodyParser(&bodyData); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON format"})
	}

	// 3. MongoDB에 저장 (Upsert: 없으면 생성, 있으면 수정)
	filter := bson.M{"user_id": userID}
	update := bson.M{
		"$set": bson.M{
			"data":       bodyData, // JSON 본문 내용
			"updated_at": time.Now(),
		},
	}
	opts := options.Update().SetUpsert(true)

	_, err := weaponCollection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		log.Printf("MongoDB Weapon Save Error: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to save weapon data"})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Weapon data saved to DB",
	})
}

// 무기 데이터 다운로드 (MongoDB -> Unity)
func GetUserWeapon(c *fiber.Ctx) error {
	targetUserID := c.Params("user_id")
	if targetUserID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "user_id is required"})
	}

	var result WeaponDoc
	err := weaponCollection.FindOne(ctx, bson.M{"user_id": targetUserID}).Decode(&result)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			return c.Status(404).JSON(fiber.Map{"error": "Weapon data not found"})
		}
		log.Printf("MongoDB Weapon Load Error: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to load weapon data"})
	}

	// [수정된 부분] 깔끔한 JSON Map으로 변환 후 전송
	cleanData := NormalizeMongoData(result.Data)
	return c.JSON(cleanData)
}

// ... 기존 코드 하단에 추가 ...

/// ========== Inventory JSON Data Handlers ==========

// 인벤토리 업로드 (Unity -> MongoDB)
func UploadUserInventory(c *fiber.Ctx) error {
	userID := getAuthedUserID(c)
	if userID == "" {
		return c.Status(401).JSON(fiber.Map{"error": "Unauthorized"})
	}

	// 1. 요청 바디(JSON) 파싱
	// Unity에서 보낸 {"items": [...]} 구조를 그대로 맵으로 받습니다.
	var bodyData map[string]interface{}
	if err := c.BodyParser(&bodyData); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON format"})
	}

	// 2. MongoDB에 저장 (Upsert: 없으면 생성, 있으면 수정)
	filter := bson.M{"user_id": userID}
	update := bson.M{
		"$set": bson.M{
			"data":       bodyData, // {"items": ...} 전체를 저장
			"updated_at": time.Now(),
		},
	}
	opts := options.Update().SetUpsert(true)

	_, err := inventoryCollection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		log.Printf("MongoDB Inventory Save Error: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to save inventory"})
	}

	return c.JSON(fiber.Map{
		"status":  "success",
		"message": "Inventory saved to DB",
	})
}

// [중요] MongoDB BSON(primitive.D)을 일반 Map/Slice로 변환하는 재귀 함수
func NormalizeMongoData(data interface{}) interface{} {
	// 1. primitive.D (Key-Value 리스트)인 경우 -> Map으로 변환
	if d, ok := data.(primitive.D); ok {
		m := make(map[string]interface{})
		for _, e := range d {
			m[e.Key] = NormalizeMongoData(e.Value)
		}
		return m
	}

	// 2. primitive.A (배열)인 경우 -> Slice로 변환
	if a, ok := data.(primitive.A); ok {
		newA := make([]interface{}, len(a))
		for i, v := range a {
			newA[i] = NormalizeMongoData(v)
		}
		return newA
	}

	// 3. 그 외 기본 타입은 그대로 반환
	return data
}

// 인벤토리 다운로드 (MongoDB -> Unity)
func GetUserInventory(c *fiber.Ctx) error {
	targetUserID := c.Params("user_id")
	if targetUserID == "" {
		return c.Status(400).JSON(fiber.Map{"error": "user_id is required"})
	}

	var result InventoryDoc
	err := inventoryCollection.FindOne(ctx, bson.M{"user_id": targetUserID}).Decode(&result)

	if err != nil {
		if err == mongo.ErrNoDocuments {
			// [수정 전] 빈 리스트를 200 OK로 반환했음
			// return c.JSON(fiber.Map{"items": []interface{}{}})

			// [수정 후] 명시적으로 404 Not Found 반환
			return c.Status(404).JSON(fiber.Map{"error": "Inventory not found"})
		}
		log.Printf("MongoDB Inventory Load Error: %v", err)
		return c.Status(500).JSON(fiber.Map{"error": "Failed to load inventory"})
	}

	// ... (나머지 로직 동일) ...
	cleanData := NormalizeMongoData(result.Data)
	if _, ok := cleanData.([]interface{}); ok {
		return c.JSON(fiber.Map{
			"items": cleanData,
		})
	}
	return c.JSON(cleanData)
}

func processUserLogs(userID string) {
	opts := options.Find().SetSort(bson.D{{Key: "timestamp", Value: 1}})
	cursor, err := logCollection.Find(ctx, bson.M{"user_id": userID, "is_processed": false}, opts)
	if err != nil {
		return
	}

	var logs []RawLog
	if err = cursor.All(ctx, &logs); err != nil {
		return
	}
	if len(logs) < 5 {
		fmt.Printf("User %s: 로그 부족 (%d개), 스킵.\n", userID, len(logs))
		return
	}

	var prevStory Story
	prevStoryText := "없음 (이번이 첫 모험입니다.)"
	findOptions := options.FindOne().SetSort(bson.D{{Key: "created_at", Value: -1}})
	if err := storyCollection.FindOne(ctx, bson.M{"user_id": userID}, findOptions).Decode(&prevStory); err == nil {
		prevStoryText = fmt.Sprintf("제목: %s\n내용: %s", prevStory.Title, prevStory.Content)
	}

	var userProfile UserProfile
	nickname := "이름 모를 모험가"
	if err := userCollection.FindOne(ctx, bson.M{"user_id": userID}).Decode(&userProfile); err == nil {
		if userProfile.Nickname != "" {
			nickname = userProfile.Nickname
		}
	}

	logText := ""
	for _, l := range logs {
		detailBytes, _ := json.Marshal(l.Detail)
		logText += fmt.Sprintf("- [%s] %s: %s\n", l.Timestamp.Format("15:04"), l.EventType, string(detailBytes))
	}

	storyContent, storyTitle := callGemini(nickname, logText, prevStoryText)
	if storyContent == "" {
		fmt.Println("✖ 스토리 생성 실패로 인해 저장하지 않습니다.")
		return
	}

	newStory := Story{
		UserID:    userID,
		Title:     storyTitle,
		Content:   storyContent,
		CreatedAt: time.Now(),
	}
	_, _ = storyCollection.InsertOne(ctx, newStory)

	var logIDs []primitive.ObjectID
	for _, l := range logs {
		logIDs = append(logIDs, l.ID)
	}
	_, _ = logCollection.UpdateMany(ctx,
		bson.M{"_id": bson.M{"$in": logIDs}},
		bson.M{"$set": bson.M{"is_processed": true}},
	)

	fmt.Printf("✔ User %s: 스토리 생성 완료!\n", userID)
}

func callGemini(nickname, logData string, prevStory string) (string, string) {
	apiKey := os.Getenv("GEMINI_API_KEY")
	if apiKey == "" {
		log.Println("WARNING: GEMINI_API_KEY가 설정되지 않았습니다.")
		return "", ""
	}

	fullPrompt := fmt.Sprintf(`
[Role]
당신은 판타지 소설 작가입니다. 
주어진 '이전 이야기'와 새로운 '게임 로그'를 연결하여 자연스럽게 이어지는 후속편을 써주세요.

[Context]
- 주인공 이름: %s
- 이전 이야기 요약: 
%s

[New Data]
- 새로운 게임 로그:
%s

[Constraint]
1. 이전 이야기의 사건이나 획득한 아이템을 언급하며 자연스럽게 이어가세요.
2. 첫 줄에는 소설의 '제목'만 적으세요.
3. 둘째 줄부터 본문을 적으세요.
4. 너무 길지 않게(500자 내외) 작성하세요.
`, nickname, prevStory, logData)

	reqBody := GeminiRequest{
		Contents: []GeminiContent{
			{Parts: []GeminiPart{{Text: fullPrompt}}},
		},
	}
	jsonData, _ := json.Marshal(reqBody)

	url := "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=" + apiKey
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", ""
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", ""
	}

	var geminiResp GeminiResponse
	if err := json.Unmarshal(bodyBytes, &geminiResp); err != nil {
		return "", ""
	}
	if len(geminiResp.Candidates) == 0 || len(geminiResp.Candidates[0].Content.Parts) == 0 {
		return "", ""
	}

	fullText := geminiResp.Candidates[0].Content.Parts[0].Text
	lines := strings.SplitN(fullText, "\n", 2)
	title := strings.TrimSpace(lines[0])
	content := ""
	if len(lines) > 1 {
		content = strings.TrimSpace(lines[1])
	} else {
		content = title
		title = "무제"
	}
	title = strings.Trim(title, "\"'# *")
	return content, title
}
