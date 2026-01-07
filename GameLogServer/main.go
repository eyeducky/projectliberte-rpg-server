package main

import (
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
	ExternalID   string             `bson:"external_id" json:"external_id"` // Custom ID sign-in에 들어갈 고유값
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

// / ========== 전역 변수(DB) ==========
var (
	ctx = context.Background()

	userCollection  *mongo.Collection
	authCollection  *mongo.Collection
	logCollection   *mongo.Collection
	storyCollection *mongo.Collection

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

	url := fmt.Sprintf("https://services.api.unity.com/auth/v1/token-exchange?projectId=%s&environmentId=%s", projectID, envID)
	req, _ := http.NewRequest("POST", url, nil)
	req.Header.Set("Authorization", "Basic "+cred)

	client := &http.Client{Timeout: 20 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("token-exchange 실패: %d / %s", resp.StatusCode, string(bodyBytes))
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
	app.Use(cors.New(cors.Config{
		AllowOrigins: "*",
		AllowHeaders: "Origin, Content-Type, Accept, Authorization, UnityEnvironment, ProjectId",
	}))

	/// ---- Auth (public) ----
	app.Post("/api/auth/register", Register)
	app.Post("/api/auth/login", Login)

	/// ---- User APIs (protected) ----
	app.Get("/api/user/profile/:id", RequireUnityAuth, GetUserProfile)
	app.Post("/api/user/nickname", RequireUnityAuth, UpdateUserNickname)
	app.Post("/api/user/class", RequireUnityAuth, UpdateUserClass)
	app.Post("/api/user/stats", RequireUnityAuth, UpdateUserStats)

	app.Post("/api/log", RequireUnityAuth, IngestLog)
	app.Get("/api/stories/:user_id", RequireUnityAuth, GetUserStories)
	app.Post("/api/upload/image", RequireUnityAuth, UploadImageToS3)

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

// 닉네임 등록/수정
func UpdateUserNickname(c *fiber.Ctx) error {
	var req UserProfile
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
			"user_nickname":   req.Nickname, // ✅ 버그 수정(기존 nickname -> user_nickname)
			"user_class":      req.UserClass,
			"skill_icon_url":  req.SkillIconURL,
			"weapon_icon_url": req.WeaponIconURL,
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
