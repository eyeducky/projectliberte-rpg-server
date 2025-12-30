package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/joho/godotenv"
	"github.com/robfig/cron/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

/// [데이터 모델]

// 유저 데이터
type UserProfile struct {
	UserID        string `json:"user_id" bson:"user_id"`       // Unity PlayerID
	Nickname      string `json:"nickname" bson:"nickname"`     // 닉네임
	CreatedAt     int64  `json:"created_at" bson:"created_at"` // 가입일
	WeaponIconURL string `json:"weapon_icon_url" bson:"weapon_icon_url"`
	SkillIconURL  string `json:"skill_icon_url" bson:"skill_icon_url"`
}

// Unity에서 받을 로그 데이터
type RawLog struct {
	ID          primitive.ObjectID `bson:"_id,omitempty" json:"id"`
	UserID      string             `bson:"user_id" json:"user_id"`
	EventType   string             `bson:"event_type" json:"event_type"`
	Detail      interface{}        `bson:"detail" json:"detail"`
	Timestamp   time.Time          `bson:"timestamp" json:"timestamp"`
	IsProcessed bool               `bson:"is_processed" json:"is_processed"` // 처리 여부 체크
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

// [전역 변수 (DB 컬렉션)]
var userCollection *mongo.Collection
var logCollection *mongo.Collection
var storyCollection *mongo.Collection
var ctx = context.Background()

var s3Client *s3.Client

// OpenAI API 키
const GeminiAPIKey = ""

func main() {

	// .env 파일 로드
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

	// 연결 테스트 (Ping)
	fmt.Println("Connecting DB...")
	if err := client.Ping(ctx, nil); err != nil {
		log.Fatal("✖ MongoDB Atlas 연결 실패: ", err)
	}

	// DB 및 컬렉션 설정
	db := client.Database("GameDB")
	userCollection = db.Collection("users")
	logCollection = db.Collection("user_raw_logs")
	storyCollection = db.Collection("user_stories")
	fmt.Println("✔ MongoDB Connected")

	initS3()

	// 스케줄러 설정 (배치 작업)
	c := cron.New()
	// "@every 3h" -> 3시간마다 실행
	// "@every 1m" -> 5분마다 실행
	c.AddFunc("@every 5m", GenerateStoriesBatchJob)
	c.Start()
	fmt.Print("⏱ Cron Scheduler Started\n\n")

	// 웹 서버 설정 (Fiber)
	app := fiber.New()
	app.Use(logger.New()) // 로깅 미들웨어
	app.Use(cors.New())   // CORS 허용

	// API 라우트
	fmt.Println("[Server Log]: Getting Data from DB on [/api/user/profile/:id]")
	app.Get("/api/user/profile/:id", GetUserProfile)
	fmt.Println("[Server Log]: Posting Data on [/api/user/nickname]")
	app.Post("/api/user/nickname", UpdateUserNickname)

	fmt.Println("[Server Log]: Posting Data on [/api/log]")
	app.Post("/api/log", IngestLog)
	fmt.Println("[Server Log]: Getting Data form DB on [api/stories/:user_id]")
	app.Get("/api/stories/:user_id", GetUserStories)

	fmt.Println("[Server Log]: Posting Image on [/api/upload/image]")
	app.Post("/api/upload/image", UploadImageToS3)

	// 서버 시작
	fmt.Println("[Server]: 플레이어의 로그를 기반한 AI 이야기 생성은 5분마다 생성됩니다.")
	fmt.Println("[Server]: 해당 플레이어의 로그의 개수가 5개 미만일 시 이야기는 생성되지 않습니다.")
	fmt.Println("[Server]: 나중에 추가적으로 AI생성을 시작하는 트리거나 로직을 추가할 예정입니다.")
	fmt.Print("\n--------------------------------------------------\n")
	fmt.Print(" 🦆-Made by Jade Ducky-🦆\n")
	fmt.Println("--------------------------------------------------")
	fmt.Print("\n[Server Log]: Server Started\n")
	log.Fatal(app.Listen(":8000"))
}

// 유저 프로필 조회
func GetUserProfile(c *fiber.Ctx) error {
	userID := c.Params("id")
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

func initS3() {
	// 환경 변수에서 키 가져오기
	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
	region := os.Getenv("AWS_REGION")

	if accessKey == "" || secretKey == "" || region == "" {
		log.Fatal("X AWS 환경변수(AccessKey, SecretKey, Region)가 설정되지 않았습니다.")
	}

	// 자격 증명 로드
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")),
	)
	if err != nil {
		log.Fatal("X AWS 설정 로드 실패: ", err)
	}

	s3Client = s3.NewFromConfig(cfg)
	fmt.Println("✔ AWS S3 Client Connected")
}

func UploadImageToS3(c *fiber.Ctx) error {
	// 1. 이미지 파일 받기
	file, err := c.FormFile("image")
	if err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "이미지 파일을 찾을 수 없습니다."})
	}

	src, err := file.Open()
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "파일을 열 수 없습니다."})
	}
	defer src.Close()

	// 2. 파라미터 받기 (UserID, ImageType)
	userID := c.FormValue("user_id")
	imageType := c.FormValue("image_type") // "weapon" 또는 "skill"

	if userID == "" || (imageType != "weapon" && imageType != "skill") {
		return c.Status(400).JSON(fiber.Map{"error": "user_id 또는 올바른 image_type(weapon/skill)이 필요합니다."})
	}

	// 3. 파일명 및 DB 필드 결정 (타입에 따라 고정 이름 사용 -> 자동 덮어쓰기)
	ext := filepath.Ext(file.Filename)
	if ext == "" {
		ext = ".png"
	} // 확장자 없으면 기본 png

	var s3Filename string
	var dbField string

	if imageType == "weapon" {
		s3Filename = "weapon_icon" + ext
		dbField = "weapon_icon_url"
	} else {
		s3Filename = "skill_icon" + ext
		dbField = "skill_icon_url"
	}

	// S3 경로: 유저ID/weapon_icon.png (폴더 정리)
	s3Key := fmt.Sprintf("%s/%s", userID, s3Filename)

	// 4. S3 업로드
	bucketName := os.Getenv("S3_BUCKET_NAME")
	_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(s3Key),
		Body:        src,
		ContentType: aws.String(file.Header.Get("Content-Type")),
	})
	if err != nil {
		return c.Status(500).JSON(fiber.Map{"error": "S3 업로드 실패", "detail": err.Error()})
	}

	// URL 생성
	region := os.Getenv("AWS_REGION")
	fileURL := fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", bucketName, region, s3Key)
	fmt.Printf("✔ [%s] 이미지 업로드 완료: %s\n", imageType, fileURL)

	// 5. MongoDB 업데이트 (선택된 필드만 수정)
	filter := bson.M{"user_id": userID}
	update := bson.M{
		"$set": bson.M{
			dbField: fileURL, // weapon_icon_url 또는 skill_icon_url 만 업데이트
		},
	}

	_, err = userCollection.UpdateOne(ctx, filter, update)
	if err != nil {
		log.Println("⚠ DB 업데이트 실패:", err)
		return c.Status(500).JSON(fiber.Map{"status": "upload_success_but_db_failed", "image_url": fileURL})
	}

	return c.JSON(fiber.Map{
		"status":    "success",
		"type":      imageType,
		"image_url": fileURL,
	})
}

// 닉네임 등록/수정
func UpdateUserNickname(c *fiber.Ctx) error {
	var req UserProfile
	if err := c.BodyParser(&req); err != nil {
		return c.Status(400).SendString("Invalid JSON")
	}

	filter := bson.M{"user_id": req.UserID}
	update := bson.M{
		"$set": bson.M{
			"nickname":   req.Nickname,
			"created_at": time.Now().Unix(),
		},
	}
	opts := options.Update().SetUpsert(true)
	_, err := userCollection.UpdateOne(ctx, filter, update, opts)
	if err != nil {
		return c.Status(500).SendString("Save Failed")
	}
	return c.JSON(fiber.Map{"status": "success", "nickname": req.Nickname})
}

// 로그 저장 (Unity -> Go)
func IngestLog(c *fiber.Ctx) error {
	var logData RawLog
	if err := c.BodyParser(&logData); err != nil {
		return c.Status(400).JSON(fiber.Map{"error": "Invalid JSON"})
	}

	// 서버 시간으로 타임스탬프 덮어쓰기
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

	// 최신순 정렬
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

/// AI 배치 프로세서

// 이야기 생성
func GenerateStoriesBatchJob() {
	fmt.Println("🚀 [Batch] 이야기 생성 작업 시작...")

	// 처리되지 않은 로그가 있는 유저 목록 찾기
	users, err := logCollection.Distinct(ctx, "user_id", bson.M{"is_processed": false})
	if err != nil {
		log.Println("Error finding users:", err)
		return
	}

	for _, u := range users {
		userID := u.(string)
		processUserLogs(userID)
	}
}

// 로그처리
func processUserLogs(userID string) {

	// 해당 유저의 미처리 로그 가져오기 (오래된 순서대로)
	opts := options.Find().SetSort(bson.D{{Key: "timestamp", Value: 1}})
	cursor, err := logCollection.Find(ctx, bson.M{
		"user_id":      userID,
		"is_processed": false,
	}, opts)

	if err != nil {
		print(err)
		return
	}

	var logs []RawLog
	if err = cursor.All(ctx, &logs); err != nil {
		return
	}

	// 로그가 너무 적으면 스킵 (5개 미만)
	if len(logs) < 5 {
		fmt.Printf("User %s: 로그 부족 (%d개), 스킵.\n", userID, len(logs))
		return
	}

	// 이전 이야기로 변환된 로그 조회 (최근 1개만)
	var prevStory Story
	var prevStoryText string = "없음 (이번이 첫 모험입니다.)"

	findOptions := options.FindOne().SetSort(bson.D{{Key: "created_at", Value: -1}}) // CreatedAt 역순(-1)으로 정렬해서 1개만 가져옴
	err = storyCollection.FindOne(ctx, bson.M{"user_id": userID}, findOptions).Decode(&prevStory)

	if err == nil {
		// 이전 이야기가 존재한다면
		prevStoryText = fmt.Sprintf("제목: %s\n내용: %s", prevStory.Title, prevStory.Content)
		fmt.Printf("   -> 이전 에피소드 발견: %s\n", prevStory.Title)
	}

	// 유저 ID 기반으로 닉네임 조회
	var userProfile UserProfile
	var nickname string
	err = userCollection.FindOne(ctx, bson.M{"user_id": userID}).Decode(&userProfile)
	if err != nil {
		nickname = "이름 모를 모험가" // 닉네임이 없거나 DB 에러 시 기본값
	} else {
		nickname = userProfile.Nickname
	}
	fmt.Printf("User %s (%s): 로그 %d개 -> Gemini AI 요청 중...\n", userID, nickname, len(logs))

	// 프롬프트 생성 (로그를 텍스트로 변환)
	logText := ""
	for _, l := range logs {
		// Go의 interface{}를 JSON 문자열로 예쁘게 변환
		detailBytes, _ := json.Marshal(l.Detail)
		logText += fmt.Sprintf("- [%s] %s: %s\n", l.Timestamp.Format("15:04"), l.EventType, string(detailBytes))
	}

	// AI 호출 (OpenAI)
	storyContent, storyTitle := callGemini(nickname, logText, prevStoryText)

	if storyContent == "" {
		fmt.Println("✖ 스토리 생성 실패로 인해 저장하지 않습니다.")
		return
	}

	// 이야기 DB 저장
	newStory := Story{
		UserID:    userID,
		Title:     storyTitle,
		Content:   storyContent,
		CreatedAt: time.Now(),
	}
	storyCollection.InsertOne(ctx, newStory)

	// 사용한 로그 처리 완료 표시 (IsProcessed = true)
	// 처리에 사용된 모든 로그의 ID를 수집
	var logIDs []primitive.ObjectID
	for _, l := range logs {
		logIDs = append(logIDs, l.ID)
	}

	_, err = logCollection.UpdateMany(ctx,
		bson.M{"_id": bson.M{"$in": logIDs}},         // WHERE _id IN (...)
		bson.M{"$set": bson.M{"is_processed": true}}, // SET is_processed = true
	)

	if err != nil {
		log.Printf("Error updating logs: %v", err)
	} else {
		fmt.Printf("✔ User %s: 스토리 생성 완료!\n", userID)
	}
}

// Google Gemini API 호출 함수
func callGemini(nickname, logData string, prevStory string) (string, string) {
	// API 키 체크
	apiKey := os.Getenv("GEMINI_API_KEY")
	if apiKey == "" {
		apiKey = GeminiAPIKey // 상단 상수 사용
	}

	if apiKey == "" {
		log.Println("WARNING: Gemini API Key가 설정되지 않았습니다.")
		return "API Key 설정 필요", "오류"
	}

	// 프롬프트 구성
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

	// JSON 요청 바디 생성
	reqBody := GeminiRequest{
		Contents: []GeminiContent{
			{Parts: []GeminiPart{{Text: fullPrompt}}},
		},
	}
	jsonData, _ := json.Marshal(reqBody)

	// HTTP 요청 (Gemini 2.0 Flash)
	url := "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=" + apiKey
	req, _ := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("✖ Gemini API 호출 실패: %v", err)
		return "", ""
	}
	defer resp.Body.Close()

	// 응답 파싱
	bodyBytes, _ := ioutil.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		log.Printf("✖ Gemini 에러 응답: %s", string(bodyBytes))
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

	// 제목/본문 분리
	lines := strings.SplitN(fullText, "\n", 2)
	title := strings.TrimSpace(lines[0])
	content := ""
	if len(lines) > 1 {
		content = strings.TrimSpace(lines[1])
	} else {
		content = title
		title = "무제"
	}

	// 제목 클린업 (특수문자 제거)
	title = strings.Trim(title, "\"'# *")

	return content, title
}
