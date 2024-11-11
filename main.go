package main

import (
	"archive/zip"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"html"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

var EDINETAPIKey string
var EDINETSubAPIKey string
var s3Client *s3.Client
var dynamoClient *dynamodb.Client
var tableName string
var bucketName string
var EDINETBucketName string
// Lambda 用に /tmp を足す
var failedJSONFile = filepath.Join("/tmp", "failed.json")
var failedReports []FailedReport
var mu sync.Mutex
var errMsg string
var emptyStrConvErr = `strconv.Atoi: parsing "": invalid syntax`
// Lambda 用に /tmp を足す
var invalidSummaryJSONFile = filepath.Join("/tmp", "invalid-summary.json")
var invalidSummaries []InvalidSummary
var apiTimes int
var registerSingleReport string
var env string

func init() {
	env = os.Getenv("ENV")
	fmt.Println("環境: ", env)

	if env == "local" {
		err := godotenv.Load()
		if err != nil {
			log.Fatal("Error loading .env file err: ", err)
			return
		}
	}
	EDINETAPIKey = os.Getenv("EDINET_API_KEY")
	if EDINETAPIKey == "" {
		fmt.Println("EDINET API key not found")
		return
	}
	EDINETSubAPIKey = os.Getenv("EDINET_SUB_API_KEY")
	if EDINETSubAPIKey == "" {
		fmt.Println("EDINET Sub API key not found")
		return
	}

	cfg, cfgErr := config.LoadDefaultConfig(context.TODO())
	if cfgErr != nil {
		fmt.Println("Load default config error: %v", cfgErr)
		return
	}
	region := os.Getenv("REGION")
	sdkConfig, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		fmt.Println(err)
		return
	}
	s3Client = s3.NewFromConfig(sdkConfig)
	dynamoClient = dynamodb.NewFromConfig(cfg)
	tableName = os.Getenv("DYNAMO_TABLE_NAME")
	bucketName = os.Getenv("BUCKET_NAME")
	EDINETBucketName = os.Getenv("EDINET_BUCKET_NAME")
	registerSingleReport = os.Getenv("REGISTER_SINGLE_REPORT")
}

func handler(ctx context.Context) {
	start := time.Now()

	if tableName == "" {
		log.Fatal("テーブル名が設定されていません")
	}

	reports, err := GetReports()
	fmt.Println("len(reports): ", len(reports))
	if err != nil {
		fmt.Println(err)
		return
	}

	// invalid-summary.json と failed.json を /tmp ディレクトリに書き込む
	// invalid-summary.json
	invalidSummaryKey := "invalid-summary.json"
	invalidOutput, err := GetS3Object(s3Client, bucketName, invalidSummaryKey)
	if err != nil {
		fmt.Println("invalid-summary.json を S3 から取得した際のエラー: ", err)
		return
	}
	invalidBody, err := io.ReadAll(invalidOutput.Body)
	if err != nil {
		fmt.Println("invalid-summary.json を S3 から取得した際の io.ReadAll エラー: ", err)
		return
	}
	defer invalidOutput.Body.Close()
	err = os.WriteFile(fmt.Sprintf("/tmp/%s", invalidSummaryKey), invalidBody, 0777)
	if err != nil {
		fmt.Println("invalid-summary.json を S3 から取得した際の os.WriteFile エラー: ", err)
		return
	}
	// failed.json
	failedKey := "failed.json"
	failedOutput, err := GetS3Object(s3Client, bucketName, failedKey)
	if err != nil {
		fmt.Println("failed.json を S3 から取得した際のエラー: ", err)
		return
	}
	failedBody, err := io.ReadAll(failedOutput.Body)
	if err != nil {
		fmt.Println("failed.json を S3 から取得した際の io.ReadAll エラー: ", err)
		return
	}
	defer failedOutput.Body.Close()
	err = os.WriteFile(fmt.Sprintf("/tmp/%s", failedKey), failedBody, 0777)
	if err != nil {
		fmt.Println("failed.json を S3 から取得した際の os.WriteFile エラー: ", err)
		return
	}

	var wg sync.WaitGroup
	for _, report := range reports {
		// 並列で処理する場合
		wg.Add(1)

		EDINETCode := report.EdinetCode
		companyName := report.FilerName
		docID := report.DocId
		var periodStart string
		var periodEnd string
		if report.PeriodStart == "" || report.PeriodEnd == "" {
			// 正規表現を用いて抽出
			periodPattern := `(\d{4}/\d{2}/\d{2})－(\d{4}/\d{2}/\d{2})`
			// 正規表現をコンパイル
			re := regexp.MustCompile(periodPattern)
			// 正規表現でマッチした部分を取得
			match := re.FindString(report.DocDescription)
			if match != "" {
				splitPeriod := strings.Split(match, "－")
				if len(splitPeriod) >= 2 {
					periodStart = strings.ReplaceAll(splitPeriod[0], "/", "-")
					periodEnd = strings.ReplaceAll(splitPeriod[1], "/", "-")
				}
			}
		}

		if report.PeriodStart != "" {
			periodStart = report.PeriodStart
		}

		if report.PeriodEnd != "" {
			periodEnd = report.PeriodEnd
		}

		// ファンダメンタルズ
		fundamental := Fundamental{
			CompanyName:     companyName,
			PeriodStart:     periodStart,
			PeriodEnd:       periodEnd,
			Sales:           0,
			OperatingProfit: 0,
			Liabilities:     0,
			NetAssets:       0,
		}
		// 並列で処理する場合
		go RegisterReport(dynamoClient, EDINETCode, docID, report.DateKey, companyName, periodStart, periodEnd, &fundamental, &wg)
	}
	// 並列で処理する場合
	wg.Wait()

	fmt.Println("All processes done ⭐️")
	fmt.Println("APIを叩いた回数(概算): ", apiTimes)
	fmt.Println("所要時間: ", time.Since(start))
}

func main() {
	fmt.Println("main start")
	if env == "local" {
		fmt.Println("ローカルです⭐️")
		handler(context.TODO())
		fmt.Println("ローカルでの処理が完了しました⭐️")
	} else if env == "production" {
		lambda.Start(handler)
	}
}

func unzip(source, destination string) (string, error) {
	// ZIPファイルをオープン
	r, err := zip.OpenReader(source)
	if err != nil {
		return "", fmt.Errorf("failed to open zip file: %v", err)
	}
	defer r.Close()

	var XBRLFilepath string

	// ZIP内の各ファイルを処理
	for _, f := range r.File {
		extension := filepath.Ext(f.Name)
		underPublic := strings.Contains(f.Name, "PublicDoc")

		// ファイル名に EDINETコードが含まれる かつ 拡張子が .xbrl の場合のみ処理する
		if underPublic && extension == ".xbrl" {
			// ファイル名を作成
			fpath := filepath.Join(destination, f.Name)

			// ディレクトリの場合は作成
			if f.FileInfo().IsDir() {
				os.MkdirAll(fpath, os.ModePerm)
				continue
			}

			// ファイルの場合はファイルを作成し、内容をコピー
			if err := os.MkdirAll(filepath.Dir(fpath), os.ModePerm); err != nil {
				return "", err
			}

			outFile, err := os.OpenFile(fpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
			if err != nil {
				return "", err
			}

			rc, err := f.Open()
			if err != nil {
				return "", err
			}

			_, err = io.Copy(outFile, rc)

			// リソースを閉じる
			outFile.Close()
			rc.Close()

			if err != nil {
				return "", err
			}

			XBRLFilepath = f.Name
		}
	}
	// zipファイルを削除
	err = os.RemoveAll(source)
	if err != nil {
		fmt.Println("zip ファイル削除エラー: ", err)
	}
	return XBRLFilepath, nil
}

/*
EDINET 書類一覧取得 API を使用し有価証券報告書または訂正有価証券報告書のデータを取得する
*/
func GetReports() ([]Result, error) {
	loc, err := time.LoadLocation("Asia/Tokyo")
	if err != nil {
		fmt.Println("load location error")
		return nil, err
	}

	var results []Result
	/*
	  【末日】
	    Jan: 31   Feb: 28   Mar: 31   Apr: 30   May: 31   Jun: 30
	    Jul: 31   Aug: 31   Sep: 30   Oct: 31   Nov: 30   Dec: 31
	*/

	var date time.Time
	var endDate time.Time

	if env == "local" {
		// 集計開始日付
		date = time.Date(2024, time.November, 11, 1, 0, 0, 0, loc)
		// 集計終了日付
		endDate = time.Date(2024, time.November, 11, 1, 0, 0, 0, loc)
	} else if env == "production" {
		today := time.Now().In(loc)
		// 集計開始日付
		date = today.AddDate(0, 0, -1).In(loc)
		// 集計終了日付
		endDate = today
	}
	for date.Before(endDate) || date.Equal(endDate) {
		fmt.Println(fmt.Sprintf("%s の処理を開始します⭐️", date.Format("2006-01-02")))

		var statement Report

		dateStr := date.Format("2006-01-02")

		dateKey := date.Format("20060102")

		url := fmt.Sprintf("https://api.edinet-fsa.go.jp/api/v2/documents.json?date=%s&&Subscription-Key=%s&type=2", dateStr, EDINETSubAPIKey)
		resp, err := http.Get(url)
		if err != nil {
			fmt.Println("http get error : ", err)
			return nil, err
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		err = json.Unmarshal(body, &statement)
		if err != nil {
			return nil, err
		}

		for _, s := range statement.Results {
			// 有価証券報告書 (Securities Report)
			isSecReport := s.FormCode == "030000" && s.DocTypeCode == "120"
			// 訂正有価証券報告書 (Amended Securities Report)
			isAmendReport := s.FormCode == "030001" && s.DocTypeCode == "130"

			if isSecReport || isAmendReport {
				s.DateKey = dateKey
				results = append(results, s)
			}
		}

		date = date.AddDate(0, 0, 1)
	}
	return results, nil
}

func RegisterReport(dynamoClient *dynamodb.Client, EDINETCode string, docID string, dateKey string, companyName string, periodStart string, periodEnd string, fundamental *Fundamental, wg *sync.WaitGroup) {
	fmt.Printf("===== ⭐️「%s」⭐️ =====\n", companyName)
	// 並列で処理する場合
	defer wg.Done()

	BSFileNamePattern := fmt.Sprintf("%s-%s-BS-from-%s-to-%s", EDINETCode, docID, periodStart, periodEnd)
	PLFileNamePattern := fmt.Sprintf("%s-%s-PL-from-%s-to-%s", EDINETCode, docID, periodStart, periodEnd)

	client := &http.Client{
		Timeout: 300 * time.Second,
	}

	dateDocKey := fmt.Sprintf("%s/%s", dateKey, docID)
	// 末尾にスラッシュを追加
	dateDocKeyWithSlash := dateDocKey + "/"
	isDocRegistered, err := CheckExistsS3Key(s3Client, EDINETBucketName, dateDocKeyWithSlash)
	if err != nil {
		fmt.Println("CheckExistsS3Key error: ", err)
		return
	}
	fmt.Printf("%s の %s は登録済みですか❓: %v\n", EDINETBucketName, dateDocKeyWithSlash, isDocRegistered)

	// XBRLファイルの中身
	var body []byte
	var parentPath string
	if isDocRegistered {
		getXBRLFromS3 := os.Getenv("GET_XBRL_FROM_S3")
		var xbrlFileName string
		if getXBRLFromS3 == "true" {
			if registerSingleReport == "true" {
				// S3 に登録済みの XBRL ファイルを取得し、中身を body に格納
				xbrlFileName = os.Getenv("XBRL_FILE_NAME")
			} else {
				// S3 をチェック
				dateDocIDKey := fmt.Sprintf("%s/%s", dateKey, docID)

				listOutput := ListS3Objects(s3Client, EDINETBucketName, dateDocIDKey)
				if len(listOutput.Contents) > 0 {
					firstFile := listOutput.Contents[0]
					// S3 に登録済みのxbrlファイル
					splitBySlash := strings.Split(*firstFile.Key, "/")
					if len(splitBySlash) >= 3 {
						xbrlFileName = splitBySlash[len(splitBySlash)-1]
					}
				}
			}
			key := fmt.Sprintf("%s/%s/%s", dateKey, docID, xbrlFileName)
			fmt.Println("S3 から XBRL ファイルを取得します⭐️")
			output, err := GetS3Object(s3Client, EDINETBucketName, key)
			if err != nil {
				log.Fatal("S3からのXBRLファイル取得エラー: ", err)
			}
			readBody, err := io.ReadAll(output.Body)
			if err != nil {
				log.Fatal("io.ReadAll エラー: ", err)
			}
			defer output.Body.Close()
			body = readBody
		}
	} else {
		fmt.Printf("「%s」のレポート (%s) を API から取得します🎾\n", companyName, docID)
		apiTimes += 1
		url := fmt.Sprintf("https://api.edinet-fsa.go.jp/api/v2/documents/%s?type=1&Subscription-Key=%s", docID, EDINETSubAPIKey)
		resp, err := client.Get(url)
		if err != nil {
			errMsg = "http get error : "
			registerFailedJson(docID, dateKey, errMsg+err.Error())
			return
		}
		defer resp.Body.Close()

		// Lambda 用に /tmp を足す
		dirPath := filepath.Join("/tmp", "XBRL")
		fmt.Println("XBRL DirPath ⭐️: ", dirPath)
		zipFileName := fmt.Sprintf("%s.zip", docID)
		path := filepath.Join(dirPath, zipFileName)

		// ディレクトリが存在しない場合は作成
		err = os.MkdirAll(dirPath, 0777)
		if err != nil {
			errMsg = "Error creating XBRL directory: "
			registerFailedJson(docID, dateKey, errMsg+err.Error())
			return
		}
		file, err := os.Create(path)
		if err != nil {
			errMsg = "Error while creating the file: "
			registerFailedJson(docID, dateKey, errMsg+err.Error())
			return
		}
		defer file.Close()

		// レスポンスのBody（ZIPファイルの内容）をファイルに書き込む
		_, err = io.Copy(file, resp.Body)
		if err != nil {
			errMsg = "Error while saving the file: "
			registerFailedJson(docID, dateKey, errMsg+err.Error())
			return
		}

		// ZIPファイルを解凍
		unzipDst := filepath.Join(dirPath, docID)
		XBRLFilepath, err := unzip(path, unzipDst)
		if err != nil {
			errMsg = "Error unzipping file: "
			registerFailedJson(docID, dateKey, errMsg+err.Error())
			return
		}

		// XBRLファイルの取得
		// Lambda 用に /tmp を足す
		parentPath = filepath.Join("/tmp", "XBRL", docID, XBRLFilepath)
		XBRLFile, err := os.Open(parentPath)
		if err != nil {
			errMsg = "XBRL open err: "
			registerFailedJson(docID, dateKey, errMsg+err.Error())
			return
		}
		// ここに xbrl ファイルがあるのでは❓
		body, err = io.ReadAll(XBRLFile)
		if err != nil {
			errMsg = "XBRL read err: "
			registerFailedJson(docID, dateKey, errMsg+err.Error())
			return
		}
	}
	// xbrlKey = 20060102/{DocID}/~~~~.xbrl
	splitBySlash := strings.Split(parentPath, "/")
	xbrlFile := splitBySlash[len(splitBySlash)-1]
	xbrlKey := fmt.Sprintf("%s/%s/%s", dateKey, docID, xbrlFile)
	fmt.Println("S3 に登録する xbrlファイルパス: ", xbrlKey)
	// S3 送信処理
	PutXBRLtoS3(docID, dateKey, xbrlKey, body)

	var xbrl XBRL
	err = xml.Unmarshal(body, &xbrl)
	if err != nil {
		errMsg = "XBRL Unmarshal err: "
		registerFailedJson(docID, dateKey, errMsg+err.Error())
		return
	}

	// 【連結貸借対照表】
	consolidatedBSPattern := `(?s)<jpcrp_cor:ConsolidatedBalanceSheetTextBlock contextRef="CurrentYearDuration">(.*?)</jpcrp_cor:ConsolidatedBalanceSheetTextBlock>`
	consolidatedBSRe := regexp.MustCompile(consolidatedBSPattern)
	consolidatedBSMatches := consolidatedBSRe.FindString(string(body))

	// 【貸借対照表】
	soloBSPattern := `(?s)<jpcrp_cor:BalanceSheetTextBlock contextRef="CurrentYearDuration">(.*?)</jpcrp_cor:BalanceSheetTextBlock>`
	soloBSRe := regexp.MustCompile(soloBSPattern)
	soloBSMatches := soloBSRe.FindString(string(body))

	// 【連結損益計算書】
	consolidatedPLPattern := `(?s)<jpcrp_cor:ConsolidatedStatementOfIncomeTextBlock contextRef="CurrentYearDuration">(.*?)</jpcrp_cor:ConsolidatedStatementOfIncomeTextBlock>`
	consolidatedPLRe := regexp.MustCompile(consolidatedPLPattern)
	consolidatedPLMatches := consolidatedPLRe.FindString(string(body))

	// 【損益計算書】
	soloPLPattern := `(?s)<jpcrp_cor:StatementOfIncomeTextBlock contextRef="CurrentYearDuration">(.*?)</jpcrp_cor:StatementOfIncomeTextBlock>`
	soloPLRe := regexp.MustCompile(soloPLPattern)
	soloPLMatches := soloPLRe.FindString(string(body))

	// 【連結キャッシュ・フロー計算書】
	consolidatedCFPattern := `(?s)<jpcrp_cor:ConsolidatedStatementOfCashFlowsTextBlock contextRef="CurrentYearDuration">(.*?)</jpcrp_cor:ConsolidatedStatementOfCashFlowsTextBlock>`
	consolidatedCFRe := regexp.MustCompile(consolidatedCFPattern)
	consolidatedCFMattches := consolidatedCFRe.FindString(string(body))
	// 【連結キャッシュ・フロー計算書 (IFRS)】
	consolidatedCFIFRSPattern := `(?s)<jpigp_cor:ConsolidatedStatementOfCashFlowsIFRSTextBlock contextRef="CurrentYearDuration">(.*?)</jpigp_cor:ConsolidatedStatementOfCashFlowsIFRSTextBlock>`
	consolidatedCFIFRSRe := regexp.MustCompile(consolidatedCFIFRSPattern)
	consolidatedCFIFRSMattches := consolidatedCFIFRSRe.FindString(string(body))

	// 【キャッシュ・フロー計算書】
	soloCFPattern := `(?s)<jpcrp_cor:StatementOfCashFlowsTextBlock contextRef="CurrentYearDuration">(.*?)</jpcrp_cor:StatementOfCashFlowsTextBlock>`
	soloCFRe := regexp.MustCompile(soloCFPattern)
	soloCFMattches := soloCFRe.FindString(string(body))

	// 【キャッシュ・フロー計算書 (IFRS)】
	soloCFIFRSPattern := `(?s)<jpcrp_cor:StatementOfCashFlowsIFRSTextBlock contextRef="CurrentYearDuration">(.*?)</jpcrp_cor:StatementOfCashFlowsIFRSTextBlock>`
	soloCFIFRSRe := regexp.MustCompile(soloCFIFRSPattern)
	soloCFIFRSMattches := soloCFIFRSRe.FindString(string(body))

	// 貸借対照表HTMLをローカルに作成
	doc, err := CreateHTML(docID, dateKey, "BS", consolidatedBSMatches, soloBSMatches, consolidatedPLMatches, soloPLMatches, BSFileNamePattern, PLFileNamePattern)
	if err != nil {
		errMsg = "PL CreateHTML エラー: "
		registerFailedJson(docID, dateKey, errMsg+err.Error())
		return
	}
	// 損益計算書HTMLをローカルに作成
	plDoc, err := CreateHTML(docID, dateKey, "PL", consolidatedBSMatches, soloBSMatches, consolidatedPLMatches, soloPLMatches, BSFileNamePattern, PLFileNamePattern)
	if err != nil {
		errMsg = "PL CreateHTML エラー: "
		registerFailedJson(docID, dateKey, errMsg+err.Error())
		return
	}

	// 貸借対照表データ
	var summary Summary
	summary.CompanyName = companyName
	summary.PeriodStart = periodStart
	summary.PeriodEnd = periodEnd
	UpdateSummary(doc, docID, dateKey, &summary, fundamental)
	// BS バリデーション用
	// isSummaryValid := ValidateSummary(summary)

	// 損益計算書データ
	var plSummary PLSummary
	plSummary.CompanyName = companyName
	plSummary.PeriodStart = periodStart
	plSummary.PeriodEnd = periodEnd
	UpdatePLSummary(plDoc, docID, dateKey, &plSummary, fundamental)
	isPLSummaryValid := ValidatePLSummary(plSummary)

	// CF計算書データ
	cfFileNamePattern := fmt.Sprintf("%s-%s-CF-from-%s-to-%s", EDINETCode, docID, periodStart, periodEnd)
	cfHTML, err := CreateCFHTML(docID, dateKey, cfFileNamePattern, string(body), consolidatedCFMattches, consolidatedCFIFRSMattches, soloCFMattches, soloCFIFRSMattches)
	if err != nil {
		errMsg = "CreateCFHTML err: "
		registerFailedJson(docID, dateKey, errMsg+err.Error())
		return
	}
	var cfSummary CFSummary
	cfSummary.CompanyName = companyName
	cfSummary.PeriodStart = periodStart
	cfSummary.PeriodEnd = periodEnd
	UpdateCFSummary(docID, dateKey, cfHTML, &cfSummary)
	isCFSummaryValid := ValidateCFSummary(cfSummary)

	// CF計算書バリデーション後

	// 並列で処理する場合
	var putFileWg sync.WaitGroup
	putFileWg.Add(2)

	// CF HTML は バリデーションの結果に関わらず送信
	// S3 に CF HTML 送信 (HTML はスクレイピング処理があるので S3 への送信処理を個別で実行)

	// 並列で処理する場合
	go PutFileToS3(docID, dateKey, EDINETCode, companyName, cfFileNamePattern, "html", &putFileWg)

	if isCFSummaryValid {
		// S3 に JSON 送信
		// 並列で処理する場合
		go HandleRegisterJSON(docID, dateKey, EDINETCode, companyName, cfFileNamePattern, cfSummary, &putFileWg)

		// TODO: invalid-summary.json から削除
		deleteInvalidSummaryJsonItem(docID, dateKey, "CF", companyName)
	} else {
		// 無効なサマリーデータをjsonに書き出す
		registerInvalidSummaryJson(docID, dateKey, "CF", companyName)

		// 並列で処理する場合
		putFileWg.Done()

		///// ログを出さない場合はコメントアウト /////
		PrintValidatedSummaryMsg(companyName, cfFileNamePattern, cfSummary, isCFSummaryValid)
		////////////////////////////////////////
	}

	// 並列で処理する場合
	putFileWg.Wait()

	// 貸借対照表バリデーションなしバージョン
	_, err = CreateJSON(docID, dateKey, BSFileNamePattern, summary)
	if err != nil {
		errMsg = "BS JSON ファイル作成エラー: "
		registerFailedJson(docID, dateKey, errMsg+err.Error())
		return
	}

	// 並列で処理する場合
	var putBsWg sync.WaitGroup
	putBsWg.Add(2)

	// BS JSON 送信
	// 並列で処理する場合
	go PutFileToS3(docID, dateKey, EDINETCode, companyName, BSFileNamePattern, "json", &putBsWg)

	// BS HTML 送信
	// 並列で処理する場合
	go PutFileToS3(docID, dateKey, EDINETCode, companyName, BSFileNamePattern, "html", &putBsWg)
	putBsWg.Wait()

	// 損益計算書バリデーション後
	// 並列で処理する場合
	var putPlWg sync.WaitGroup
	putPlWg.Add(2)

	// PL HTML 送信 (バリデーション結果に関わらず)
	// 並列で処理する場合
	go PutFileToS3(docID, dateKey, EDINETCode, companyName, PLFileNamePattern, "html", &putPlWg)

	if isPLSummaryValid {
		_, err = CreateJSON(docID, dateKey, PLFileNamePattern, plSummary)
		if err != nil {
			errMsg = "PL JSON ファイル作成エラー: "
			registerFailedJson(docID, dateKey, errMsg+err.Error())
			return
		}
		// PL JSON 送信
		// 並列で処理する場合
		go PutFileToS3(docID, dateKey, EDINETCode, companyName, PLFileNamePattern, "json", &putPlWg)

		// TODO: invalid-summary.json から削除
		deleteInvalidSummaryJsonItem(docID, dateKey, "PL", companyName)
	} else {
		// 無効なサマリーデータをjsonに書き出す
		registerInvalidSummaryJson(docID, dateKey, "PL", companyName)

		// 並列で処理する場合
		wg.Done()
	}
	// 並列で処理する場合
	putPlWg.Wait()

	// XBRL ファイルの削除
	xbrlDir := filepath.Join("XBRL", docID)
	err = os.RemoveAll(xbrlDir)
	if err != nil {
		errMsg = "XBRL ディレクトリ削除エラー: "
		registerFailedJson(docID, dateKey, errMsg+err.Error())
	}

	// ファンダメンタル用jsonの送信
	if ValidateFundamentals(*fundamental) {
		RegisterFundamental(dynamoClient, docID, dateKey, *fundamental, EDINETCode)

		// invalid-summary.json から削除
		deleteInvalidSummaryJsonItem(docID, dateKey, "Fundamentals", companyName)
	} else {
		// 無効なサマリーデータをjsonに書き出す
		registerInvalidSummaryJson(docID, dateKey, "Fundamentals", companyName)
	}

	// レポートの登録処理完了後、failed.json から該当のデータを削除する
	deleteFailedJsonItem(docID, dateKey, companyName)
	// レポートの登録処理完了後、invalid-summary.json から該当のデータを削除する

	fmt.Printf("「%s」のレポート(%s)の登録処理完了⭐️\n", companyName, docID)
}

func UpdateSummary(doc *goquery.Document, docID string, dateKey string, summary *Summary, fundamental *Fundamental) {
	doc.Find("tr").Each(func(i int, s *goquery.Selection) {
		tdText := s.Find("td").Text()
		tdText = strings.TrimSpace(tdText)
		splitTdTexts := strings.Split(tdText, "\n")
		var titleTexts []string
		for _, t := range splitTdTexts {
			if t != "" {
				titleTexts = append(titleTexts, t)
			}
		}
		if len(titleTexts) >= 3 {
			titleName := titleTexts[0]

			// 前期
			previousText := titleTexts[1]
			previousIntValue, err := ConvertTextValue2IntValue(previousText)
			if err != nil {
				if err.Error() != emptyStrConvErr {
					errMsg = "ConvertTextValue2IntValue (BS previous) エラー: "
					registerFailedJson(docID, dateKey, errMsg+err.Error())
				}
				return
			}

			// 当期
			currentText := titleTexts[2]
			currentIntValue, err := ConvertTextValue2IntValue(currentText)
			if err != nil {
				if err.Error() != emptyStrConvErr {
					errMsg = "ConvertTextValue2IntValue (BS current) エラー: "
					registerFailedJson(docID, dateKey, errMsg+err.Error())
				}
				return
			}

			if titleName == "流動資産合計" {
				summary.CurrentAssets.Previous = previousIntValue
				summary.CurrentAssets.Current = currentIntValue
			}
			if titleName == "有形固定資産合計" {
				summary.TangibleAssets.Previous = previousIntValue
				summary.TangibleAssets.Current = currentIntValue
			}
			if titleName == "無形固定資産合計" {
				summary.IntangibleAssets.Previous = previousIntValue
				summary.IntangibleAssets.Current = currentIntValue
			}
			if titleName == "投資その他の資産合計" {
				summary.InvestmentsAndOtherAssets.Previous = previousIntValue
				summary.InvestmentsAndOtherAssets.Current = currentIntValue
			}
			if titleName == "流動負債合計" {
				summary.CurrentLiabilities.Previous = previousIntValue
				summary.CurrentLiabilities.Current = currentIntValue
			}
			if titleName == "固定負債合計" {
				summary.FixedLiabilities.Previous = previousIntValue
				summary.FixedLiabilities.Current = currentIntValue
			}
			if titleName == "純資産合計" {
				summary.NetAssets.Previous = previousIntValue
				summary.NetAssets.Current = currentIntValue
				// fundamental
				fundamental.NetAssets = currentIntValue
			}
			if titleName == "負債合計" {
				// fundamental
				fundamental.Liabilities = currentIntValue
			}
		}

		if len(splitTdTexts) == 1 && titleTexts != nil && strings.Contains(titleTexts[0], "単位：") {
			baseStr := splitTdTexts[0]
			baseStr = strings.ReplaceAll(baseStr, "(", "")
			baseStr = strings.ReplaceAll(baseStr, "（", "")
			baseStr = strings.ReplaceAll(baseStr, ")", "")
			baseStr = strings.ReplaceAll(baseStr, "）", "")
			splitUnitStrs := strings.Split(baseStr, "：")
			if len(splitUnitStrs) >= 2 {
				summary.UnitString = splitUnitStrs[1]
			}
		}
	})
}

func UpdatePLSummary(doc *goquery.Document, docID string, dateKey string, plSummary *PLSummary, fundamental *Fundamental) {
	// 営業収益合計の設定が終わったかどうか管理するフラグ
	isOperatingRevenueDone := false
	// 営業費用合計の設定が終わったかどうか管理するフラグ
	isOperatingCostDone := false

	doc.Find("tr").Each(func(i int, s *goquery.Selection) {
		tdText := s.Find("td").Text()
		tdText = strings.TrimSpace(tdText)
		splitTdTexts := strings.Split(tdText, "\n")
		var titleTexts []string
		for _, t := range splitTdTexts {
			if t != "" {
				titleTexts = append(titleTexts, t)
			}
		}
		if len(titleTexts) >= 3 {
			titleName := titleTexts[0]

			// 前期
			previousText := titleTexts[1]
			previousIntValue, err := ConvertTextValue2IntValue(previousText)
			if err != nil {
				if err.Error() != emptyStrConvErr {
					errMsg = "ConvertTextValue2IntValue (PL previous) エラー: "
					registerFailedJson(docID, dateKey, errMsg+err.Error())
				}
				return
			}

			// 当期
			currentText := titleTexts[2]
			currentIntValue, err := ConvertTextValue2IntValue(currentText)
			if err != nil {
				if err.Error() != emptyStrConvErr {
					errMsg = "ConvertTextValue2IntValue (PL current) エラー: "
					registerFailedJson(docID, dateKey, errMsg+err.Error())
				}
				return
			}

			// switch titleName {
			// case "売上原価":
			//   plSummary.CostOfGoodsSold.Previous = previousIntValue
			//   plSummary.CostOfGoodsSold.Current = currentIntValue
			// case "販売費及び一般管理費":
			//   plSummary.SGAndA.Previous = previousIntValue
			//   plSummary.SGAndA.Current = currentIntValue
			// case "売上高":
			//   plSummary.Sales.Previous = previousIntValue
			//   plSummary.Sales.Current = currentIntValue
			//   // fundamental
			//   fundamental.Sales = currentIntValue
			// }
			if strings.Contains(titleName, "売上原価") {
				plSummary.CostOfGoodsSold.Previous = previousIntValue
				plSummary.CostOfGoodsSold.Current = currentIntValue
			}
			if strings.Contains(titleName, "販売費及び一般管理費") {
				plSummary.SGAndA.Previous = previousIntValue
				plSummary.SGAndA.Current = currentIntValue
			}
			if strings.Contains(titleName, "売上高") {
				plSummary.Sales.Previous = previousIntValue
				plSummary.Sales.Current = currentIntValue
				// fundamental
				fundamental.Sales = currentIntValue
			}
			if strings.Contains(titleName, "営業利益") {
				plSummary.OperatingProfit.Previous = previousIntValue
				plSummary.OperatingProfit.Current = currentIntValue
				// fundamental
				fundamental.OperatingProfit = currentIntValue
			}
			if titleName == "営業損失（△）" {
				// fmt.Println("営業損失とだけ書いてあります")
				if plSummary.OperatingProfit.Previous == 0 {
					plSummary.OperatingProfit.Previous = previousIntValue
				}
				if plSummary.OperatingProfit.Current == 0 {
					plSummary.OperatingProfit.Current = currentIntValue
				}
				// fundamental
				if fundamental.OperatingProfit == 0 {
					fundamental.OperatingProfit = currentIntValue
				}
			}
			// 営業収益の後に営業収益合計がある場合は上書き
			if strings.Contains(titleName, "営業収益") && !isOperatingRevenueDone {
				plSummary.HasOperatingRevenue = true
				plSummary.OperatingRevenue.Previous = previousIntValue
				plSummary.OperatingRevenue.Current = currentIntValue
				// fundamental
				fundamental.HasOperatingRevenue = true
				fundamental.OperatingRevenue = currentIntValue
				if titleName == "営業収益合計" {
					isOperatingRevenueDone = true
				}
			}
			// 営業費用の後に営業費用合計がある場合は上書き
			if strings.Contains(titleName, "営業費用") && !isOperatingCostDone {
				plSummary.HasOperatingCost = true
				plSummary.OperatingCost.Previous = previousIntValue
				plSummary.OperatingCost.Current = currentIntValue
				// fundamental
				fundamental.HasOperatingCost = true
				fundamental.OperatingCost = currentIntValue
				if titleName == "営業費用合計" {
					isOperatingCostDone = true
				}
			}
		}
		if len(splitTdTexts) == 1 && titleTexts != nil && strings.Contains(titleTexts[0], "単位：") {
			baseStr := splitTdTexts[0]
			baseStr = strings.ReplaceAll(baseStr, "(", "")
			baseStr = strings.ReplaceAll(baseStr, ")", "")
			splitUnitStrs := strings.Split(baseStr, "：")
			if len(splitUnitStrs) >= 2 {
				plSummary.UnitString = splitUnitStrs[1]
			}
		}
	})
}

func ValidateSummary(summary Summary) bool {
	if summary.CompanyName != "" &&
		summary.PeriodStart != "" &&
		summary.PeriodEnd != "" &&
		(summary.CurrentAssets.Previous != 0 || summary.CurrentAssets.Current != 0) &&
		(summary.TangibleAssets.Previous != 0 || summary.TangibleAssets.Current != 0) &&
		(summary.IntangibleAssets.Previous != 0 || summary.IntangibleAssets.Current != 0) &&
		(summary.InvestmentsAndOtherAssets.Previous != 0 || summary.InvestmentsAndOtherAssets.Current != 0) &&
		(summary.CurrentLiabilities.Previous != 0 || summary.CurrentLiabilities.Current != 0) &&
		(summary.FixedLiabilities.Previous != 0 || summary.FixedLiabilities.Current != 0) &&
		(summary.NetAssets.Previous != 0 || summary.NetAssets.Current != 0) {
		return true
	}
	return false
}

func ValidatePLSummary(plSummary PLSummary) bool {
	if plSummary.HasOperatingRevenue && plSummary.HasOperatingCost {
		// 営業費用がある場合、営業費用と営業利益があればいい
		if (plSummary.OperatingRevenue.Previous != 0 || plSummary.OperatingRevenue.Current != 0) &&
			(plSummary.OperatingCost.Previous != 0 || plSummary.OperatingCost.Current != 0) {
			return true
		}
	} else if plSummary.CompanyName != "" &&
		plSummary.PeriodStart != "" &&
		plSummary.PeriodEnd != "" &&
		(plSummary.CostOfGoodsSold.Previous != 0 || plSummary.CostOfGoodsSold.Current != 0) &&
		(plSummary.SGAndA.Previous != 0 || plSummary.SGAndA.Current != 0) &&
		(plSummary.Sales.Previous != 0 || plSummary.Sales.Current != 0) &&
		(plSummary.OperatingProfit.Previous != 0 || plSummary.OperatingProfit.Current != 0) {
		return true
	}
	return false
}

func RegisterCompany(dynamoClient *dynamodb.Client, EDINETCode string, companyName string, isSummaryValid bool, isPLSummaryValid bool) {
	foundItems, err := QueryByName(dynamoClient, tableName, companyName, EDINETCode)
	if err != nil {
		fmt.Println(err)
		return
	}

	if len(foundItems) == 0 {
		var company Company
		id, uuidErr := uuid.NewUUID()
		if uuidErr != nil {
			fmt.Println("uuid create error")
			return
		}
		company.ID = id.String()
		company.EDINETCode = EDINETCode
		company.Name = companyName
		if isSummaryValid {
			company.BS = 1
		}
		if isPLSummaryValid {
			company.PL = 1
		}

		item, err := attributevalue.MarshalMap(company)
		if err != nil {
			fmt.Println("MarshalMap err: ", err)
			return
		}

		input := &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item:      item,
		}
		_, err = dynamoClient.PutItem(context.TODO(), input)
		if err != nil {
			fmt.Println("dynamoClient.PutItem err: ", err)
			return
		}
		doneMsg := fmt.Sprintf("「%s」をDBに新規登録しました ⭕️", companyName)
		fmt.Println(doneMsg)
	} else {
		foundItem := foundItems[0]
		if foundItem != nil {
			var company Company
			// BS, PL フラグの設定
			// fmt.Println("すでに登録された company: ", foundItem)
			// company型に UnmarshalMap
			err = attributevalue.UnmarshalMap(foundItem, &company)
			if err != nil {
				fmt.Println("attributevalue.UnmarshalMap err: ", err)
				return
			}

			if company.BS == 0 && isSummaryValid {
				// company.BS を 1 に更新
				// UpdateBS(dynamoClient, company.ID, 1)
			}

			if company.PL == 0 && isPLSummaryValid {
				// company.PL を 1 に更新
				// UpdatePL(dynamoClient, company.ID, 1)
			}
		}
	}
}

func UpdateBS(dynamoClient *dynamodb.Client, id string, bs int) {
	// 更新するカラムとその値の指定
	updateInput := &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: id},
		},
		UpdateExpression: aws.String("SET #bs = :newBS"),
		ExpressionAttributeNames: map[string]string{
			"#bs": "bs", // "bs" カラムを指定
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":newBS": &types.AttributeValueMemberN{Value: strconv.Itoa(bs)},
		},
		ReturnValues: types.ReturnValueUpdatedNew, // 更新後の新しい値を返す
	}

	// 更新の実行
	_, err := dynamoClient.UpdateItem(context.TODO(), updateInput)
	if err != nil {
		log.Fatalf("failed to update item, %v", err)
	}
}

func UpdatePL(dynamoClient *dynamodb.Client, id string, pl int) {
	// 更新するカラムとその値の指定
	updateInput := &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: id},
		},
		UpdateExpression: aws.String("SET #pl = :newPL"),
		ExpressionAttributeNames: map[string]string{
			"#pl": "pl", // "pl" カラムを指定
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":newPL": &types.AttributeValueMemberN{Value: strconv.Itoa(pl)},
		},
		ReturnValues: types.ReturnValueUpdatedNew, // 更新後の新しい値を返す
	}

	// 更新の実行
	_, err := dynamoClient.UpdateItem(context.TODO(), updateInput)
	if err != nil {
		log.Fatalf("failed to update item, %v", err)
	}
}

func RegisterFundamental(dynamoClient *dynamodb.Client, docID string, dateKey string, fundamental Fundamental, EDINETCode string) {
	fundamentalBody, err := json.Marshal(fundamental)
	if err != nil {
		errMsg = "fundamental json.Marshal err: "
		registerFailedJson(docID, dateKey, errMsg+err.Error())
		return
	}
	// ファイル名
	fundamentalsFileName := fmt.Sprintf("%s-fundamentals-from-%s-to-%s.json", EDINETCode, fundamental.PeriodStart, fundamental.PeriodEnd)
	key := fmt.Sprintf("%s/Fundamentals/%s", EDINETCode, fundamentalsFileName)
	// ファイルの存在チェック
	existsFile, _ := s3Client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if existsFile == nil {
		_, err = s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket:      aws.String(bucketName),
			Key:         aws.String(key),
			Body:        strings.NewReader(string(fundamentalBody)),
			ContentType: aws.String("application/json"),
		})
		if err != nil {
			// fmt.Println(err)
			errMsg = "fundamentals ファイルの S3 Put Object エラー: "
			registerFailedJson(docID, dateKey, errMsg+err.Error())
			return
		}
		///// ログを出さない場合はコメントアウト /////
		uploadDoneMsg := fmt.Sprintf("「%s」のファンダメンタルズJSONを登録しました ⭕️ (ファイル名: %s)", fundamental.CompanyName, key)
		fmt.Println(uploadDoneMsg)
		////////////////////////////////////////
	}
}

func ValidateFundamentals(fundamental Fundamental) bool {
	if fundamental.HasOperatingRevenue && fundamental.HasOperatingCost {
		if fundamental.CompanyName != "" &&
			fundamental.PeriodStart != "" &&
			fundamental.PeriodEnd != "" &&
			fundamental.OperatingProfit != 0 &&
			fundamental.Liabilities != 0 &&
			fundamental.NetAssets != 0 &&
			fundamental.OperatingRevenue != 0 &&
			fundamental.OperatingCost != 0 {
			return true
		}
	} else if fundamental.CompanyName != "" &&
		fundamental.PeriodStart != "" &&
		fundamental.PeriodEnd != "" &&
		fundamental.Sales != 0 &&
		fundamental.OperatingProfit != 0 &&
		fundamental.Liabilities != 0 &&
		fundamental.NetAssets != 0 {
		return true
	}
	return false
}

/*
HTMLをパースしローカルに保存する
@params

		fileType:                BS もしくは PL
		body:                    ファイルの中身
		consolidatedBSMatches:   連結貸借対照表データが入っているタグの中身
		soloBSMatches:           貸借対照表データが入っているタグの中身
		consolidatedPLMatches:   連結損益計算書データが入っているタグの中身
		soloPLMatches:           損益計算書データが入っているタグの中身
	  BSFileNamePattern:       BSファイル名パターン
		PLFileNamePattern:       PLファイル名パターン
*/
func CreateHTML(docID string, dateKey string, fileType, consolidatedBSMatches, soloBSMatches, consolidatedPLMatches, soloPLMatches, BSFileNamePattern, PLFileNamePattern string) (*goquery.Document, error) {
	// エスケープ文字をデコード
	var unescapedStr string

	// BS の場合
	if fileType == "BS" {
		if consolidatedBSMatches == "" && soloBSMatches == "" {
			registerFailedJson(docID, dateKey, "parse 対象の貸借対照表データがありません")
			return nil, errors.New("parse 対象の貸借対照表データがありません")
		} else if consolidatedBSMatches != "" {
			unescapedStr = html.UnescapeString(consolidatedBSMatches)
		} else if soloBSMatches != "" {
			unescapedStr = html.UnescapeString(soloBSMatches)
		}
	}

	// PL の場合
	if fileType == "PL" {
		if consolidatedPLMatches == "" && soloPLMatches == "" {
			registerFailedJson(docID, dateKey, "parse 対象の損益計算書データがありません")
			return nil, errors.New("parse 対象の損益計算書データがありません")
		} else if consolidatedPLMatches != "" {
			unescapedStr = html.UnescapeString(consolidatedPLMatches)
		} else if soloPLMatches != "" {
			unescapedStr = html.UnescapeString(soloPLMatches)
		}
	}

	// デコードしきれていない文字は replace
	// 特定のエンティティをさらに手動でデコード
	unescapedStr = strings.ReplaceAll(unescapedStr, "&apos;", "'")

	// HTMLデータを加工
	unescapedStr = FormatHtmlTable(unescapedStr)

	// html ファイルとして書き出す
	// Lambda 用に /tmp を足す
	HTMLDirName := filepath.Join("/tmp", "HTML")
	var fileName string
	var filePath string

	if fileType == "BS" {
		fileName = fmt.Sprintf("%s.html", BSFileNamePattern)
		filePath = filepath.Join(HTMLDirName, fileName)
	}

	if fileType == "PL" {
		fileName = fmt.Sprintf("%s.html", PLFileNamePattern)
		filePath = filepath.Join(HTMLDirName, fileName)
	}

	// HTMLディレクトリが存在するか確認
	if _, err := os.Stat(HTMLDirName); os.IsNotExist(err) {
		// ディレクトリが存在しない場合は作成
		err := os.Mkdir(HTMLDirName, 0755) // 0755はディレクトリのパーミッション
		if err != nil {
			errMsg = "HTML ローカルディレクトリ作成エラー: "
			registerFailedJson(docID, dateKey, errMsg+err.Error())
			fmt.Println("Error creating HTML directory:", err)
			return nil, err
		}
	}

	createFile, err := os.Create(filePath)
	if err != nil {
		errMsg = "HTML ローカルファイル作成エラー: "
		registerFailedJson(docID, dateKey, errMsg+err.Error())
		// fmt.Println("HTML create err: ", err)
		return nil, err
	}
	defer createFile.Close()

	_, err = createFile.WriteString(unescapedStr)
	if err != nil {
		errMsg = "HTML ローカルファイル書き込みエラー: "
		registerFailedJson(docID, dateKey, errMsg+err.Error())
		// fmt.Println("HTML write err: ", err)
		return nil, err
	}

	openFile, err := os.Open(filePath)
	if err != nil {
		errMsg = "HTML ローカルファイル書き込み後オープンエラー: "
		registerFailedJson(docID, dateKey, errMsg+err.Error())
		// fmt.Println("HTML open error: ", err)
		return nil, err
	}
	defer openFile.Close()

	// goqueryでHTMLをパース
	doc, err := goquery.NewDocumentFromReader(openFile)
	if err != nil {
		errMsg = "HTML goquery.NewDocumentFromReader error: "
		registerFailedJson(docID, dateKey, errMsg+err.Error())
		// fmt.Println("HTML goquery.NewDocumentFromReader error: ", err)
		return nil, err
	}
	// return した doc は updateSummary に渡す
	return doc, nil
}

// CF計算書登録処理
/*
cfFileNamePattern:          ファイル名のパターン
body:                       文字列に変換したXBRLファイル
consolidatedCFMattches:     連結キャッシュ・フロー計算書
consolidatedCFIFRSMattches: 連結キャッシュ・フロー計算書 (IFRS)
soloCFMattches:             キャッシュ・フロー計算書
soloCFIFRSPattern:          キャッシュ・フロー計算書 (IFRS)
*/
func CreateCFHTML(docID string, dateKey string, cfFileNamePattern, body string, consolidatedCFMattches string, consolidatedCFIFRSMattches string, soloCFMattches string, soloCFIFRSMattches string) (*goquery.Document, error) {

	if consolidatedCFMattches == "" && consolidatedCFIFRSMattches == "" && soloCFMattches == "" && soloCFIFRSMattches == "" {
		registerFailedJson(docID, dateKey, "パースする対象がありません")
		return nil, errors.New("パースする対象がありません")
	}

	var match string

	if consolidatedCFMattches != "" {
		match = consolidatedCFMattches
	} else if consolidatedCFIFRSMattches != "" {
		match = consolidatedCFIFRSMattches
	} else if soloCFMattches != "" {
		match = soloCFMattches
	} else if soloCFIFRSMattches != "" {
		match = soloCFIFRSMattches
	}

	unescapedMatch := html.UnescapeString(match)
	// デコードしきれていない文字は replace
	// 特定のエンティティをさらに手動でデコード
	unescapedMatch = strings.ReplaceAll(unescapedMatch, "&apos;", "'")
	unescapedMatch = FormatHtmlTable(unescapedMatch)

	// Lambda 用に /tmp を足す
	HTMLDirName := filepath.Join("/tmp", "HTML")
	cfHTMLFileName := fmt.Sprintf("%s.html", cfFileNamePattern)
	cfHTMLFilePath := filepath.Join(HTMLDirName, cfHTMLFileName)

	// HTML ファイルの作成
	cfHTML, err := os.Create(cfHTMLFilePath)
	if err != nil {
		errMsg = "CF HTML create err: "
		registerFailedJson(docID, dateKey, errMsg+err.Error())
		return nil, err
	}
	defer cfHTML.Close()

	// HTML ファイルに書き込み
	_, err = cfHTML.WriteString(unescapedMatch)
	if err != nil {
		errMsg = "CF HTML write err: "
		registerFailedJson(docID, dateKey, errMsg+err.Error())
		return nil, err
	}

	// HTML ファイルの読み込み
	cfHTMLFile, err := os.Open(cfHTMLFilePath)
	if err != nil {
		errMsg = "CF HTML open error: "
		registerFailedJson(docID, dateKey, errMsg+err.Error())
		return nil, err
	}
	defer cfHTMLFile.Close()

	// goqueryでHTMLをパース
	cfDoc, err := goquery.NewDocumentFromReader(cfHTMLFile)
	if err != nil {
		errMsg = "CF goquery.NewDocumentFromReader err: "
		registerFailedJson(docID, dateKey, errMsg+err.Error())
		return nil, err
	}
	return cfDoc, nil
}

func CreateJSON(docID string, dateKey string, fileNamePattern string, summary interface{}) (string, error) {
	fileName := fmt.Sprintf("%s.json", fileNamePattern)
	// Lambda 用に /tmp を足す
	// filePath := fmt.Sprintf("json/%s", fileName)
	jsonDirName := filepath.Join("/tmp", "json")
	filePath := filepath.Join(jsonDirName, fileName)

	// ディレクトリが存在しない場合は作成
	// Lambda 用に /tmp を足す
	err := os.MkdirAll(jsonDirName, os.ModePerm)
	if err != nil {
		errMsg = "Error creating JSON directory: "
		registerFailedJson(docID, dateKey, errMsg+err.Error())
		return "", err
	}

	jsonFile, err := os.Create(filePath)
	if err != nil {
		// fmt.Println(err)
		errMsg = "ローカル JSON ファイル作成エラー: "
		registerFailedJson(docID, dateKey, errMsg+err.Error())
		return "", err
	}
	defer jsonFile.Close()

	jsonBody, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		// fmt.Println(err)
		errMsg = "ローカル JSON MarshalIndent エラー: "
		registerFailedJson(docID, dateKey, errMsg+err.Error())
		return "", err
	}
	_, err = jsonFile.Write(jsonBody)
	if err != nil {
		// fmt.Println(err)
		errMsg = "ローカル JSON ファイル write エラー: "
		registerFailedJson(docID, dateKey, errMsg+err.Error())
		return "", err
	}
	return filePath, nil
}

func UpdateCFSummary(docID string, dateKey string, cfDoc *goquery.Document, cfSummary *CFSummary) {
	cfDoc.Find("tr").Each(func(i int, s *goquery.Selection) {
		tdText := s.Find("td").Text()
		tdText = strings.TrimSpace(tdText)
		splitTdTexts := strings.Split(tdText, "\n")
		var titleTexts []string
		for _, t := range splitTdTexts {
			t = strings.TrimSpace(t)
			if t != "" {
				titleTexts = append(titleTexts, t)
			}
		}
		if len(titleTexts) >= 3 {
			titleName := titleTexts[0]

			// 前期
			previousText := titleTexts[1]

			// TODO: テスト後削除
			// if strings.Contains(titleName, "期首残高") || strings.Contains(titleName, "期末残高")  {
			//   // fmt.Printf("「%s」前期: %s⭐️\n", titleName, previousText)
			//   // fmt.Println("テキスト ⭐️: ", tdText)
			//   // fmt.Println("splitTdTexts ⭐️: ", splitTdTexts)
			//   // fmt.Println("titleTexts ⭐️: ", titleTexts)
			//   fmt.Println("==================")
			//   fmt.Println(titleTexts[0])
			//   fmt.Println(titleTexts[1])
			//   fmt.Println(titleTexts[2])

			// }
			previousIntValue, err := ConvertTextValue2IntValue(previousText)
			if err != nil {
				if err.Error() != emptyStrConvErr {
					errMsg = "ConvertTextValue2IntValue (CF previous) エラー: "
					registerFailedJson(docID, dateKey, errMsg+err.Error())
				}
				return
			}

			// 当期
			currentText := titleTexts[2]
			currentIntValue, err := ConvertTextValue2IntValue(currentText)
			if err != nil {
				if err.Error() != emptyStrConvErr {
					errMsg = "ConvertTextValue2IntValue (CF current) エラー: "
					registerFailedJson(docID, dateKey, errMsg+err.Error())
				}
				return
			}

			if strings.Contains(titleName, "営業活動による") {
				// TODO: テスト後消す
				// fmt.Println("==================")
				// fmt.Println("項目名⭐️: ", titleName)
				// fmt.Println("今期のテキスト⭐️: ", currentText)
				cfSummary.OperatingCF.Previous = previousIntValue
				cfSummary.OperatingCF.Current = currentIntValue
			}
			if strings.Contains(titleName, "投資活動による") {
				cfSummary.InvestingCF.Previous = previousIntValue
				cfSummary.InvestingCF.Current = currentIntValue
			}
			if strings.Contains(titleName, "財務活動による") {
				cfSummary.FinancingCF.Previous = previousIntValue
				cfSummary.FinancingCF.Current = currentIntValue
			}
			if strings.Contains(titleName, "期首残高") {
				cfSummary.StartCash.Previous = previousIntValue
				cfSummary.StartCash.Current = currentIntValue
			}
			if strings.Contains(titleName, "期末残高") {
				cfSummary.EndCash.Previous = previousIntValue
				cfSummary.EndCash.Current = currentIntValue
			}
		}
		if len(splitTdTexts) == 1 && titleTexts != nil && strings.Contains(titleTexts[0], "単位：") {
			formatUnitStr := FormatUnitStr(splitTdTexts[0])
			if formatUnitStr != "" {
				cfSummary.UnitString = formatUnitStr
			}
		}
	})
}

func ValidateCFSummary(cfSummary CFSummary) bool {
	if cfSummary.CompanyName != "" &&
		cfSummary.PeriodStart != "" &&
		cfSummary.PeriodEnd != "" &&
		(cfSummary.OperatingCF.Previous != 0 || cfSummary.OperatingCF.Current != 0) &&
		(cfSummary.InvestingCF.Previous != 0 || cfSummary.InvestingCF.Current != 0) &&
		(cfSummary.FinancingCF.Previous != 0 || cfSummary.FinancingCF.Current != 0) {
		// 期首残高、期末残高のチェックは外す
		// (cfSummary.StartCash.Previous != 0 || cfSummary.StartCash.Current != 0) &&
		// (cfSummary.EndCash.Previous != 0 || cfSummary.EndCash.Current != 0)
		return true
	}
	return false
}

func PrintValidatedSummaryMsg(companyName string, fileName string, summary interface{}, isValid bool) {
	var summaryType string

	switch summary.(type) {
	case Summary:
		summaryType = "貸借対照表"
	case PLSummary:
		summaryType = "損益計算書"
	case CFSummary:
		summaryType = "CF計算書"
	}

	jsonBody, _ := json.MarshalIndent(summary, "", "  ")

	var detailStr string
	var validStr string
	switch isValid {
	case true:
		validStr = "有効です⭕️"
	case false:
		validStr = "無効です❌"
		detailStr = fmt.Sprintf("詳細:\n%v\n", string(jsonBody))
	}

	msg := fmt.Sprintf("「%s」の%sサマリーJSON (%s) は%s %s", companyName, summaryType, fileName, validStr, detailStr)
	println(msg)
}

// 汎用ファイル送信処理
func PutFileToS3(docID string, dateKey string, EDINETCode string, companyName string, fileNamePattern string, extension string, wg *sync.WaitGroup) {
	// 並列で処理する場合
	defer wg.Done()

	var fileName string
	var filePath string

	switch extension {
	case "json":
		fileName = fmt.Sprintf("%s.json", fileNamePattern)
		// Lambda 用に /tmp を足す
		// filePath = fmt.Sprintf("json/%s", fileName)
		filePath = filepath.Join("/tmp", "json", fileName)
	case "html":
		fileName = fmt.Sprintf("%s.html", fileNamePattern)
		// Lambda 用に /tmp を足す
		// filePath = fmt.Sprintf("HTML/%s", fileName)
		filePath = filepath.Join("/tmp", "HTML", fileName)
	}

	// 処理後、ローカルファイルを削除
	defer func() {
		err := os.RemoveAll(filePath)
		if err != nil {
			errMsg = "ローカルファイル削除エラー: "
			registerFailedJson(docID, dateKey, errMsg+err.Error())
			return
		}
		// fmt.Printf("%s を削除しました\n", filePath)
	}()

	// S3 に ファイルを送信 (Key は aws configure で設定しておく)
	file, err := os.Open(filePath)
	if err != nil {
		errMsg = "S3 へのファイル送信時、ローカルファイル open エラー: "
		registerFailedJson(docID, dateKey, errMsg+err.Error())
		return
	}
	defer func() {
		err := file.Close()
		if err != nil {
			errMsg = "S3 へのファイル送信時、ローカルファイル close エラー: "
			registerFailedJson(docID, dateKey, errMsg+err.Error())
			return
		}
	}()

	splitByHyphen := strings.Split(fileName, "-")
	if len(splitByHyphen) >= 3 {
		reportType := splitByHyphen[2] // BS or PL or CF
		key := fmt.Sprintf("%s/%s/%s", EDINETCode, reportType, fileName)

		contentType, err := GetContentType(docID, dateKey, extension)
		if err != nil {
			errMsg = "ContentType 取得エラー: "
			registerFailedJson(docID, dateKey, errMsg+err.Error())
			return
		}

		// ファイルの存在チェック
		existsFile, _ := s3Client.HeadObject(context.TODO(), &s3.HeadObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		})
		if existsFile == nil {
			_, err = s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket:      aws.String(bucketName),
				Key:         aws.String(key),
				Body:        file,
				ContentType: aws.String(contentType),
			})
			if err != nil {
				errMsg = "S3 PutObject error: "
				registerFailedJson(docID, dateKey, errMsg+err.Error())
				return
			}

			///// ログを出さない場合はコメントアウト /////
			var reportTypeStr string
			switch reportType {
			case "BS":
				reportTypeStr = "貸借対照表"
			case "PL":
				reportTypeStr = "損益計算書"
			case "CF":
				reportTypeStr = "CF計算書"
			}
			uploadDoneMsg := fmt.Sprintf("「%s」の%s%sを登録しました ⭕️ (ファイル名: %s)", companyName, reportTypeStr, extension, key)
			fmt.Println(uploadDoneMsg)
			////////////////////////////////////////
		}
	}
}

func GetContentType(docID string, dateKey string, extension string) (string, error) {
	switch extension {
	case "json":
		return "application/json", nil
	case "html":
		return "text/html", nil
	}
	registerFailedJson(docID, dateKey, "無効なファイル形式です")
	return "", errors.New("無効なファイル形式です")
}

func HandleRegisterJSON(docID string, dateKey string, EDINETCode string, companyName string, fileNamePattern string, summary interface{}, wg *sync.WaitGroup) {
	_, err := CreateJSON(docID, dateKey, fileNamePattern, summary)
	if err != nil {
		errMsg = "CF JSON ファイル作成エラー: "
		registerFailedJson(docID, dateKey, errMsg+err.Error())
		return
	}
	PutFileToS3(docID, dateKey, EDINETCode, companyName, fileNamePattern, "json", wg)
}

func FormatUnitStr(baseStr string) string {
	baseStr = strings.ReplaceAll(baseStr, "(", "")
	baseStr = strings.ReplaceAll(baseStr, "（", "")
	baseStr = strings.ReplaceAll(baseStr, ")", "")
	baseStr = strings.ReplaceAll(baseStr, "）", "")
	splitUnitStrs := strings.Split(baseStr, "：")
	if len(splitUnitStrs) >= 2 {
		return splitUnitStrs[1]
	}
	return ""
}

func FormatHtmlTable(htmlStr string) string {
	tbPattern := `(?s)<table(.*?)>`
	tbRe := regexp.MustCompile(tbPattern)
	tbMatch := tbRe.FindString(htmlStr)

	tbWidthPatternSemicolon := `width(.*?);`
	tbWidthPatternPt := `width(.*?)pt`
	tbWidthPatternPx := `width(.*?)px`

	tbWidthReSemicolon := regexp.MustCompile(tbWidthPatternSemicolon)
	tbWidthRePt := regexp.MustCompile(tbWidthPatternPt)
	tbWidthRePx := regexp.MustCompile(tbWidthPatternPx)

	tbWidthMatchSemicolon := tbWidthReSemicolon.FindString(tbMatch)
	tbWidthMatchPt := tbWidthRePt.FindString(tbMatch)
	tbWidthMatchPx := tbWidthRePx.FindString(tbMatch)

	var newTbStr string
	if tbWidthMatchSemicolon != "" {
		newTbStr = strings.ReplaceAll(tbMatch, tbWidthMatchSemicolon, "")
	} else if tbWidthMatchPt != "" {
		newTbStr = strings.ReplaceAll(tbMatch, tbWidthMatchPt, "")
	} else if tbWidthMatchPx != "" {
		newTbStr = strings.ReplaceAll(tbMatch, tbWidthMatchPx, "")
	}

	if newTbStr != "" {
		// <table> タグを入れ替える
		htmlStr = strings.ReplaceAll(htmlStr, tbMatch, newTbStr)
	}

	// colgroup の削除
	colGroupPattern := `(?s)<colgroup(.*?)</colgroup>`
	colGroupRe := regexp.MustCompile(colGroupPattern)
	colGroupMatch := colGroupRe.FindString(htmlStr)
	if colGroupMatch != "" {
		htmlStr = strings.ReplaceAll(htmlStr, colGroupMatch, "")
	}
	return htmlStr
}

/*
エラーが出た場合に docID といつ登録されたレポートなのかをjsonファイルに記録する
*/
// TODO: S3 のファイルを操作する
func registerFailedJson(docID string, dateKey string, errMsg string) {
	fmt.Println(errMsg)
	// 取得から更新までをロック
	mu.Lock()
	// 更新まで終わったらロック解除
	defer mu.Unlock()

	// json ファイルの取得
	openFile, _ := os.Open(failedJSONFile)
	fmt.Println("JSON Open File ⭐️: ", openFile)
	defer openFile.Close()
	if openFile == nil {
		// ファイルがない場合は作成する
		_, err := os.Create(failedJSONFile)
		if err != nil {
			fmt.Println("failed json create error: ", err)
			// registerFailedJson(docID, dateKey, err.Error())
		}
	}
	// fmt.Println("failed.json file: ", file)

	// 中身を取得
	body, err := io.ReadAll(openFile)
	if err != nil {
		fmt.Println("failed json read error: ", err)
		// registerFailedJson(docID, dateKey, err.Error())
	}
	err = json.Unmarshal(body, &failedReports)
	if err != nil {
		fmt.Println("registerFailedJson")
		fmt.Println("failed json unmarshal error: ", err)
		// registerFailedJson(docID, dateKey, err.Error())
	}
	// 配列の中に自分がいるか確認
	alreadyFailed := false
	for _, report := range failedReports {
		if report.DocID == docID {
			alreadyFailed = true
		}
	}
	// 未登録であれば登録
	if !alreadyFailed {
		failedReport := FailedReport{
			DocID:        docID,
			RegisterDate: dateKey,
			ErrorMsg:     errMsg,
		}
		failedReports = append(failedReports, failedReport)
		jsonBody, err := json.MarshalIndent(failedReports, "", "  ")
		if err != nil {
			fmt.Println("json MarshalIndent when trying to write failed json error: ", err)
			// registerFailedJson(docID, dateKey, err.Error())
		}
		// json を書き出す
		err = os.WriteFile(failedJSONFile, jsonBody, 0666)
		if err != nil {
			fmt.Println("failed json write error: ", err)
			// registerFailedJson(docID, dateKey, err.Error())
		}
	}
}

// TODO: S3 のファイルを操作する
func deleteFailedJsonItem(docID string, dateKey string, companyName string) {
	mu.Lock()
	// 更新まで終わったらロック解除
	defer mu.Unlock()
	// json ファイルの取得
	openFile, _ := os.Open(failedJSONFile)
	defer openFile.Close()
	if openFile == nil {
		// ファイルがない場合は作成する
		_, err := os.Create(failedJSONFile)
		if err != nil {
			fmt.Println("failed json create error: ", err)
			// registerFailedJson(docID, dateKey, err.Error())
		}
	}
	// fmt.Println("failed.json file: ", file)

	// 中身を取得
	body, err := io.ReadAll(openFile)
	if err != nil {
		fmt.Println("failed json read error: ", err)
		// registerFailedJson(docID, dateKey, err.Error())
	}
	err = json.Unmarshal(body, &failedReports)
	if err != nil {
		fmt.Println("deleteFailedJsonItem")
		fmt.Println("failed json unmarshal error: ", err)
		// registerFailedJson(docID, dateKey, err.Error())
	}
	// 自分を取り除いたスライスを作成
	var newFailedReports []FailedReport

	alreadyFailed := false
	for _, report := range failedReports {
		if report.DocID == docID {
			alreadyFailed = true
		} else {
			newFailedReports = append(newFailedReports, report)
		}
	}
	// failed.json に登録されていれば json を登録し直す
	if alreadyFailed {
		fmt.Printf("failed.json から「%s」のレポート (%s) を削除したもので書き換えます❗️\n", companyName, docID)
		jsonBody, err := json.MarshalIndent(newFailedReports, "", "  ")
		if err != nil {
			fmt.Println("json MarshalIndent when trying to write failed json error: ", err)
			// registerFailedJson(docID, dateKey, err.Error())
		}
		// json を S3 に書き出す
		err = PutJSONObject(s3Client, bucketName, "failed.json", jsonBody)
		if err != nil {
			fmt.Println("failed json put object error: ", err)
		}
		// err = os.WriteFile(failedJSONFile, jsonBody, 0666)
		// if err != nil {
		// 	fmt.Println("failed json write error: ", err)
		// 	// registerFailedJson(docID, dateKey, err.Error())
		// }
	}
}

/*
無効なサマリーだった場合にjsonファイルに記録する
*/
// TODO: S3 のファイルを操作する
func registerInvalidSummaryJson(docID string, dateKey string, summaryType string, companyName string) {
	// 取得から更新までをロック
	mu.Lock()
	// 更新まで終わったらロック解除
	defer mu.Unlock()

	// json ファイルの取得
	openFile, _ := os.Open(invalidSummaryJSONFile)
	defer openFile.Close()
	if openFile == nil {
		// ファイルがない場合は作成する
		_, err := os.Create(invalidSummaryJSONFile)
		if err != nil {
			fmt.Println("failed json create error: ", err)
			// registerFailedJson(docID, dateKey, err.Error())
		}
	}
	// fmt.Println("failed.json file: ", file)

	// 中身を取得
	body, err := io.ReadAll(openFile)
	if err != nil {
		fmt.Println("failed json read error: ", err)
		// registerFailedJson(docID, dateKey, err.Error())
	}
	err = json.Unmarshal(body, &invalidSummaries)
	if err != nil {
		fmt.Println("registerInvalidSummaryJson")
		fmt.Println("failed json unmarshal error: ", err)
		// registerFailedJson(docID, dateKey, err.Error())
	}
	// 配列の中に自分がいるか確認
	alreadyFailed := false
	for _, report := range invalidSummaries {
		if report.DocID == docID {
			alreadyFailed = true
		}
	}
	// 未登録であれば登録
	if !alreadyFailed {
		invalidSummary := InvalidSummary{
			DocID:        docID,
			RegisterDate: dateKey,
			ErrorMsg:     errMsg,
			SummaryType:  summaryType,
			CompanyName:  companyName,
		}
		invalidSummaries = append(invalidSummaries, invalidSummary)
		jsonBody, err := json.MarshalIndent(invalidSummaries, "", "  ")
		if err != nil {
			fmt.Println("json MarshalIndent when trying to write invalid summary error: ", err)
			// registerFailedJson(docID, dateKey, err.Error())
		}
		// json を S3 に書き出す
		err = PutJSONObject(s3Client, bucketName, "invalid-summary.json", jsonBody)
		if err != nil {
			fmt.Println("invalid summary json put object error: ", err)
		}

		// err = os.WriteFile(invalidSummaryJSONFile, jsonBody, 0666)
		// if err != nil {
		// 	fmt.Println("invalid summary write error: ", err)
		// 	// registerFailedJson(docID, dateKey, err.Error())
		// }
	}
}

// TODO: S3 のファイルを操作する
func deleteInvalidSummaryJsonItem(docID string, dateKey string, summaryType string, companyName string) {
	// 取得から更新までをロック
	mu.Lock()
	// 更新まで終わったらロック解除
	defer mu.Unlock()

	// json ファイルの取得
	openFile, _ := os.Open(invalidSummaryJSONFile)
	defer openFile.Close()
	if openFile == nil {
		// ファイルがない場合は作成する
		_, err := os.Create(invalidSummaryJSONFile)
		if err != nil {
			fmt.Println("failed json create error: ", err)
			// registerFailedJson(docID, dateKey, err.Error())
		}
	}
	// fmt.Println("failed.json file: ", file)

	// 中身を取得
	body, err := io.ReadAll(openFile)
	if err != nil {
		fmt.Println("failed json read error: ", err)
		// registerFailedJson(docID, dateKey, err.Error())
	}
	err = json.Unmarshal(body, &invalidSummaries)
	if err != nil {
		fmt.Println("deleteInvalidSummaryJsonItem")
		fmt.Println("failed json unmarshal error: ", err)
		// registerFailedJson(docID, dateKey, err.Error())
	}

	// 自分を取り除いたスライスを作成
	var newInvalidSummaries []InvalidSummary

	// 配列の中に自分がいるか確認
	alreadyFailed := false
	for _, report := range invalidSummaries {
		if report.DocID == docID {
			alreadyFailed = true
		} else {
			newInvalidSummaries = append(newInvalidSummaries, report)
		}
	}
	// invalid-summary.json に登録されていれば json を登録し直す
	if alreadyFailed {
		fmt.Printf("invalid-summary.json から「%s」のレポート (%s) を削除したもので書き換えます❗️\n", companyName, docID)

		jsonBody, err := json.MarshalIndent(newInvalidSummaries, "", "  ")
		if err != nil {
			fmt.Println("json MarshalIndent when trying to write invalid summary error: ", err)
			// registerFailedJson(docID, dateKey, err.Error())
		}
		// json S3 にを書き出す
		err = PutJSONObject(s3Client, bucketName, "invalid-summary.json", jsonBody)
		if err != nil {
			fmt.Println("invalid summary json put object error: ", err)
		}

		// err = os.WriteFile(invalidSummaryJSONFile, jsonBody, 0666)
		// if err != nil {
		// 	fmt.Println("invalid summary write error: ", err)
		// 	// registerFailedJson(docID, dateKey, err.Error())
		// }
	}
}

func PutXBRLtoS3(docID string, dateKey string, key string, body []byte) {
	// ファイルの存在チェック
	existsFile, _ := s3Client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(EDINETBucketName),
		Key:    aws.String(key),
	})
	if existsFile == nil {
		_, err := s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket:      aws.String(EDINETBucketName),
			Key:         aws.String(key),
			Body:        strings.NewReader(string(body)),
			ContentType: aws.String("application/xml"),
		})
		if err != nil {
			errMsg = "S3 への XBRL ファイル送信エラー: "
			registerFailedJson(docID, dateKey, errMsg+err.Error())
			return
		}
		fmt.Printf("xbrlファイル(%s)をS3に送信しました⭐️\n", key)
	}
}
