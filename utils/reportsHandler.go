package utils

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
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
)

/* TODO

E02949-S100TAPI-BS-from-2023-01-01-to-2023-12-31.json (旧)
　　　　と
E02949-S100UPOU-BS-from-2023-01-01-to-2023-12-31.json (新)
があった場合、S3 から旧を削除し、新を送信する
*/

var EDINETAPIKey string
var EDINETSubAPIKey string
var S3Client *s3.Client
var DynamoClient *dynamodb.Client
var TableName string
var BucketName string
var EDINETBucketName string

// Lambda 用に /tmp を足す
var FailedJSONFile = filepath.Join("/tmp", "failed.json")
var FailedReports []FailedReport
var Mu sync.Mutex
var ErrMsg string
var EmptyStrConvErr = `strconv.Atoi: parsing "": invalid syntax`

// Lambda 用に /tmp を足す
var InvalidSummaryJSONFile = filepath.Join("/tmp", "invalid-summary.json")
var InvalidSummaries []InvalidSummary
var ApiTimes int
var RegisterSingleReport string
var Env string
var Parallel string
var FromToPattern = `\b(BS|CF|PL|fundamentals)-from-\d{4}-\d{2}-\d{2}-to-\d{4}-\d{2}-\d{2}\.(html|json)`
var FromToWithoutTypePattern = `-from-\d{4}-\d{2}-\d{2}-to-\d{4}-\d{2}-\d{2}\.(html|json)`
var XBRLExtensionPattern = `.xbrl`

func init() {
	Env = os.Getenv("ENV")
	fmt.Println("環境: ", Env)

	if Env == "local" {
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
	S3Client = s3.NewFromConfig(sdkConfig)
	DynamoClient = dynamodb.NewFromConfig(cfg)
	TableName = os.Getenv("DYNAMO_TABLE_NAME")
	BucketName = os.Getenv("BUCKET_NAME")
	EDINETBucketName = os.Getenv("EDINET_BUCKET_NAME")
	RegisterSingleReport = os.Getenv("REGISTER_SINGLE_REPORT")
	Parallel = os.Getenv("PARALLEL")

	// /tmp ディレクトリに invalid-summary.json と failed.json を登録する
	CreateFailedFiles()
}

func Unzip(source, destination string) (string, error) {
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

	if Env == "local" {
		// 集計開始日付
		date = time.Date(2022, time.July, 1, 1, 0, 0, 0, loc)
		// 集計終了日付
		endDate = time.Date(2022, time.July, 31, 1, 0, 0, 0, loc)
	} else if Env == "production" {
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
	if Parallel == "true" {
		defer wg.Done()
	}

	var objectKeys []string
	// compass-reports-bucket/{EDINETコード} の item をスライスに格納
	listObjectsOutput := ListS3Objects(S3Client, BucketName, EDINETCode)
	if len(listObjectsOutput.Contents) > 0 {
		for _, item := range listObjectsOutput.Contents {
			key := *item.Key
			objectKeys = append(objectKeys, key)
		}
	}

	BSFileNamePattern := fmt.Sprintf("%s-%s-BS-from-%s-to-%s", EDINETCode, docID, periodStart, periodEnd)
	PLFileNamePattern := fmt.Sprintf("%s-%s-PL-from-%s-to-%s", EDINETCode, docID, periodStart, periodEnd)

	client := &http.Client{
		Timeout: 300 * time.Second,
	}

	dateDocKey := fmt.Sprintf("%s/%s", dateKey, docID)
	// 末尾にスラッシュを追加
	dateDocKeyWithSlash := dateDocKey + "/"
	isDocRegistered, err := CheckExistsS3Key(S3Client, EDINETBucketName, dateDocKeyWithSlash)
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
			if RegisterSingleReport == "true" {
				// S3 に登録済みの XBRL ファイルを取得し、中身を body に格納
				xbrlFileName = os.Getenv("XBRL_FILE_NAME")
			} else {
				// S3 をチェック
				dateDocIDKey := fmt.Sprintf("%s/%s", dateKey, docID)

				listOutput := ListS3Objects(S3Client, EDINETBucketName, dateDocIDKey)
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
			fmt.Println("S3 から XBRL ファイルを取得します⭐️ key: ", key)
			output, _ := GetS3Object(S3Client, EDINETBucketName, key)
			// HTML ファイルを取得し、HTML ファイルもなければ return
			if output != nil {
				readBody, err := io.ReadAll(output.Body)
				if err != nil {
					fmt.Println("io.ReadAll エラー: ", err)
          return
				}
				body = readBody
				defer output.Body.Close()
			} else {
				HTMLFileKey := ConvertExtensionFromXBRLToHTML(key)
				if HTMLFileKey == "" {
					fmt.Println("元データの XBRL, HTML ファイルがないため処理を終了します❗️")
					return
				}
				fmt.Println("S3 から HTML ファイルを取得します⭐️ key: ", HTMLFileKey)
				HTMLOutput, _ := GetS3Object(S3Client, EDINETBucketName, HTMLFileKey)
				if HTMLOutput == nil {
					fmt.Println("元データの XBRL, HTML ファイルがないため処理を終了します❗️")
					return
				}
				HTMLReadBody, err := io.ReadAll(HTMLOutput.Body)
				if err != nil {
					fmt.Println("io.ReadAll エラー: ", err)
          return
				}
				body = HTMLReadBody
				defer HTMLOutput.Body.Close()
			}
		}
	} else {
		fmt.Printf("「%s」のレポート (%s) を API から取得します🎾\n", companyName, docID)
		ApiTimes += 1
		url := fmt.Sprintf("https://api.edinet-fsa.go.jp/api/v2/documents/%s?type=1&Subscription-Key=%s", docID, EDINETSubAPIKey)
		resp, err := client.Get(url)
		if err != nil {
			ErrMsg = "http get error : "
			RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
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
			ErrMsg = "Error creating XBRL directory: "
			RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
			return
		}
		file, err := os.Create(path)
		if err != nil {
			ErrMsg = "Error while creating the file: "
			RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
			return
		}
		defer file.Close()

		// レスポンスのBody（ZIPファイルの内容）をファイルに書き込む
		_, err = io.Copy(file, resp.Body)
		if err != nil {
			ErrMsg = "Error while saving the file: "
			RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
			return
		}

		// ZIPファイルを解凍
		unzipDst := filepath.Join(dirPath, docID)
		XBRLFilepath, err := Unzip(path, unzipDst)
		if err != nil {
			ErrMsg = "Error unzipping file: "
			RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
			return
		}

		// XBRLファイルの取得
		// Lambda 用に /tmp を足す
		parentPath = filepath.Join("/tmp", "XBRL", docID, XBRLFilepath)
		XBRLFile, err := os.Open(parentPath)
		if err != nil {
			ErrMsg = "XBRL open err: "
			RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
			return
		}
		// ここに xbrl ファイルがあるのでは❓
		body, err = io.ReadAll(XBRLFile)
		if err != nil {
			ErrMsg = "XBRL read err: "
			RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
			return
		}
	}
	// xbrlKey = 20060102/{DocID}/~~~~.xbrl
	splitBySlash := strings.Split(parentPath, "/")
	xbrlFile := splitBySlash[len(splitBySlash)-1]
	xbrlKey := fmt.Sprintf("%s/%s/%s", dateKey, docID, xbrlFile)
	fmt.Println("S3 に登録する xbrlファイルパス: ", xbrlKey)

	// S3 送信処理 (オリジナルHTML送信で事足りそうなのでコメントアウト)
	// PutXBRLtoS3(docID, dateKey, xbrlKey, body)
	// オリジナルHTMLを S3 に送信
	PutOriginalHTMLToS3(docID, dateKey, xbrlKey, string(body))

	var xbrl XBRL
	err = xml.Unmarshal(body, &xbrl)
	if err != nil {
		ErrMsg = "XBRL Unmarshal err: "
		RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
		return
	}

	// 【連結貸借対照表】
	consolidatedBSPattern := `(?s)<jpcrp_cor:ConsolidatedBalanceSheetTextBlock contextRef="CurrentYearDuration">(.*?)</jpcrp_cor:ConsolidatedBalanceSheetTextBlock>`
	consolidatedBSRe := regexp.MustCompile(consolidatedBSPattern)
	// fmt.Println("元データ: ", string(body))
	consolidatedBSMatches := consolidatedBSRe.FindString(string(body))

	// 【連結貸借対照表（IFRS）】※ 【連結財政状態計算書】が正式名称
	// consolidatedBSIFRSPattern := `(?s)<jpigp_cor:f contextRef="CurrentYearDuration">(.*?)</jpigp_cor:f>`
	consolidatedBSIFRSPattern := `(?s) <jpigp_cor:ConsolidatedStatementOfFinancialPositionIFRSTextBlock contextRef="CurrentYearDuration">(.*?)</jpigp_cor:ConsolidatedStatementOfFinancialPositionIFRSTextBlock>`
	consolidatedBSIFRSRe := regexp.MustCompile(consolidatedBSIFRSPattern)
	consolidatedBSIFRSMatches := consolidatedBSIFRSRe.FindString(string(body))

	// 【貸借対照表】
	soloBSPattern := `(?s)<jpcrp_cor:BalanceSheetTextBlock contextRef="CurrentYearDuration">(.*?)</jpcrp_cor:BalanceSheetTextBlock>`
	soloBSRe := regexp.MustCompile(soloBSPattern)
	soloBSMatches := soloBSRe.FindString(string(body))

	// 【連結損益計算書】
	consolidatedPLPattern := `(?s)<jpcrp_cor:ConsolidatedStatementOfIncomeTextBlock contextRef="CurrentYearDuration">(.*?)</jpcrp_cor:ConsolidatedStatementOfIncomeTextBlock>`
	consolidatedPLRe := regexp.MustCompile(consolidatedPLPattern)
	consolidatedPLMatches := consolidatedPLRe.FindString(string(body))

	// 【連結損益計算書（IFRS）】
	consolidatedPLIFRSPattern := `(?s)<jpigp_cor:ConsolidatedStatementOfProfitOrLossIFRSTextBlock contextRef="CurrentYearDuration">(.*?)</jpigp_cor:ConsolidatedStatementOfProfitOrLossIFRSTextBlock>`
	condolidatedPLIFRSRe := regexp.MustCompile(consolidatedPLIFRSPattern)
	consolidatedPLIFRSMatches := condolidatedPLIFRSRe.FindString(string(body))

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

  // 【証券コード】
  securityCodePattern := `<jpdei_cor:SecurityCodeDEI[^>]*>(\d+)<\/jpdei_cor:SecurityCodeDEI>`
  // securityCodePattern := `(?s)<jpdei_cor:SecurityCodeDEI contextRef="FilingDateInstant">(.*?)</jpdei_cor:SecurityCodeDEI>`
  securityCodeRe := regexp.MustCompile(securityCodePattern)
  // securityCodeMatches := securityCodeRe.FindString(string(body))
  securityCodeMatches := securityCodeRe.FindStringSubmatch(string(body))
  var securityCode string
  if len(securityCodeMatches) > 1 {
    // fmt.Printf("「%s」の証券コード: %s\n", companyName, securityCodeMatches[1])
    securityCode = securityCodeMatches[1]
  }

  // 証券コード登録
  err = UpdateSecCode(dynamoClient, EDINETCode, securityCode)
  if err != nil {
    fmt.Println("UpdateSecCode error: ", err)
  }

	// 貸借対照表HTMLをローカルに作成
	doc, err := CreateHTML(docID, dateKey, "BS", consolidatedBSMatches, consolidatedBSIFRSMatches, soloBSMatches, consolidatedPLMatches, consolidatedPLIFRSMatches, soloPLMatches, BSFileNamePattern, PLFileNamePattern)
	if err != nil {
		ErrMsg = "PL CreateHTML エラー: "
		RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
		return
	}
	// 損益計算書HTMLをローカルに作成
	plDoc, err := CreateHTML(docID, dateKey, "PL", consolidatedBSMatches, consolidatedBSIFRSMatches, soloBSMatches, consolidatedPLMatches, consolidatedPLIFRSMatches, soloPLMatches, BSFileNamePattern, PLFileNamePattern)
	if err != nil {
		ErrMsg = "PL CreateHTML エラー: "
		RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
		return
	}

	// 貸借対照表データ
	var summary Summary
	summary.CompanyName = companyName
	summary.PeriodStart = periodStart
	summary.PeriodEnd = periodEnd
	// UpdateEverySumary に置き換える
	// UpdateSummary(doc, docID, dateKey, &summary, fundamental)
	UpdateEverySummary(doc, docID, dateKey, "bs", &summary, nil, nil, fundamental)
	// fmt.Println("BSSummary ⭐️: ", summary)

	// BS バリデーション用
	// isSummaryValid := ValidateSummary(summary)

	// 損益計算書データ
	var plSummary PLSummary
	plSummary.CompanyName = companyName
	plSummary.PeriodStart = periodStart
	plSummary.PeriodEnd = periodEnd
	// UpdateEverySummary で置き換える
	// UpdatePLSummary(plDoc, docID, dateKey, &plSummary, fundamental)
	UpdateEverySummary(plDoc, docID, dateKey, "pl", nil, &plSummary, nil, fundamental)

	isPLSummaryValid := ValidatePLSummary(plSummary)
	// fmt.Println("PLSummary ⭐️: ", plSummary)

	// CF計算書データ
	cfFileNamePattern := fmt.Sprintf("%s-%s-CF-from-%s-to-%s", EDINETCode, docID, periodStart, periodEnd)
	cfHTML, err := CreateCFHTML(docID, dateKey, cfFileNamePattern, string(body), consolidatedCFMattches, consolidatedCFIFRSMattches, soloCFMattches, soloCFIFRSMattches)
	if err != nil {
		ErrMsg = "CreateCFHTML err: "
		RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
		return
	}
	var cfSummary CFSummary
	cfSummary.CompanyName = companyName
	cfSummary.PeriodStart = periodStart
	cfSummary.PeriodEnd = periodEnd
	// UpdateEverySummary に置き換える
	// UpdateCFSummary(docID, dateKey, cfHTML, &cfSummary)
	UpdateEverySummary(cfHTML, docID, dateKey, "cf", nil, nil, &cfSummary, nil)
	isCFSummaryValid := ValidateCFSummary(cfSummary)

	// CF計算書バリデーション後

	// 並列で処理する場合
	var putFileWg sync.WaitGroup
	if Parallel == "true" {
		putFileWg.Add(2)
	}

	// CF HTML は バリデーションの結果に関わらず送信
	// S3 に CF HTML 送信 (HTML はスクレイピング処理があるので S3 への送信処理を個別で実行)

	if Parallel == "true" {
		// 並列で処理する場合
		go PutFileToS3(docID, dateKey, EDINETCode, companyName, cfFileNamePattern, "html", objectKeys, &putFileWg)
	} else {
		// 直列で処理する場合
		PutFileToS3(docID, dateKey, EDINETCode, companyName, cfFileNamePattern, "html", objectKeys, &putFileWg)
	}

	if isCFSummaryValid {
		// S3 に JSON 送信
		if Parallel == "true" {
			// 並列で処理する場合
			go HandleRegisterJSON(docID, dateKey, EDINETCode, companyName, cfFileNamePattern, cfSummary, objectKeys, &putFileWg)
		} else {
			// 直列で処理する場合
			HandleRegisterJSON(docID, dateKey, EDINETCode, companyName, cfFileNamePattern, cfSummary, objectKeys, &putFileWg)
		}

		// TODO: invalid-summary.json から削除
		deleteInvalidSummaryJsonItem(docID, dateKey, "CF", companyName)
	} else {
		// 無効なサマリーデータをjsonに書き出す
		RegisterInvalidSummaryJson(docID, dateKey, "CF", companyName)

		// 並列で処理する場合
		if Parallel == "true" {
			putFileWg.Done()
		}

		///// ログを出さない場合はコメントアウト /////
		PrintValidatedSummaryMsg(companyName, cfFileNamePattern, cfSummary, isCFSummaryValid)
		////////////////////////////////////////
	}

	// 並列で処理する場合
	if Parallel == "true" {
		putFileWg.Wait()
	}

	// 貸借対照表バリデーションなしバージョン
	_, err = CreateJSON(docID, dateKey, BSFileNamePattern, summary)
	if err != nil {
		ErrMsg = "BS JSON ファイル作成エラー: "
		RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
		return
	}

	// 並列で処理する場合
	var putBsWg sync.WaitGroup
	if Parallel == "true" {
		putBsWg.Add(2)
	}

	// BS JSON 送信
	if Parallel == "true" {
		// 並列で処理する場合
		go PutFileToS3(docID, dateKey, EDINETCode, companyName, BSFileNamePattern, "json", objectKeys, &putBsWg)
	} else {
		// 直列で処理する場合
		PutFileToS3(docID, dateKey, EDINETCode, companyName, BSFileNamePattern, "json", objectKeys, &putBsWg)
	}

	// BS HTML 送信
	if Parallel == "true" {
		// 並列で処理する場合
		go PutFileToS3(docID, dateKey, EDINETCode, companyName, BSFileNamePattern, "html", objectKeys, &putBsWg)
	} else {
		// 直列で処理する場合
		PutFileToS3(docID, dateKey, EDINETCode, companyName, BSFileNamePattern, "html", objectKeys, &putBsWg)
	}

	// 並列で処理する場合
	if Parallel == "true" {
		putBsWg.Wait()
	}

	// 損益計算書バリデーション後
	// 並列で処理する場合
	var putPlWg sync.WaitGroup
	if Parallel == "true" {
		putPlWg.Add(2)
	}

	// PL HTML 送信 (バリデーション結果に関わらず)
	if Parallel == "true" {
		// 並列で処理する場合
		go PutFileToS3(docID, dateKey, EDINETCode, companyName, PLFileNamePattern, "html", objectKeys, &putPlWg)
	} else {
		// 直列で処理する場合
		PutFileToS3(docID, dateKey, EDINETCode, companyName, PLFileNamePattern, "html", objectKeys, &putPlWg)
	}

	if isPLSummaryValid {
		_, err = CreateJSON(docID, dateKey, PLFileNamePattern, plSummary)
		if err != nil {
			ErrMsg = "PL JSON ファイル作成エラー: "
			RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
			return
		}
		// PL JSON 送信
		if Parallel == "true" {
			// 並列で処理する場合
			go PutFileToS3(docID, dateKey, EDINETCode, companyName, PLFileNamePattern, "json", objectKeys, &putPlWg)
		} else {
			// 直列で処理する場合
			PutFileToS3(docID, dateKey, EDINETCode, companyName, PLFileNamePattern, "json", objectKeys, &putPlWg)
		}

		// TODO: invalid-summary.json から削除
		deleteInvalidSummaryJsonItem(docID, dateKey, "PL", companyName)
	} else {
		PrintValidatedSummaryMsg(companyName, "損益計算書", plSummary, isPLSummaryValid)
		// fmt.Println("PLSummary が無効です❌")
		// 無効なサマリーデータをjsonに書き出す
		RegisterInvalidSummaryJson(docID, dateKey, "PL", companyName)

		if Parallel == "true" {
			// 並列で処理する場合
			wg.Done()
		}
	}
	if Parallel == "true" {
		// 並列で処理する場合
		putPlWg.Wait()
	}

	// XBRL ファイルの削除
	xbrlDir := filepath.Join("XBRL", docID)
	err = os.RemoveAll(xbrlDir)
	if err != nil {
		ErrMsg = "XBRL ディレクトリ削除エラー: "
		RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
	}

	// ファンダメンタル用jsonの送信
	if ValidateFundamentals(*fundamental) {
		RegisterFundamental(dynamoClient, docID, dateKey, *fundamental, EDINETCode)

		// invalid-summary.json から削除
		deleteInvalidSummaryJsonItem(docID, dateKey, "Fundamentals", companyName)
	} else {
		// 無効なサマリーデータをjsonに書き出す
		RegisterInvalidSummaryJson(docID, dateKey, "Fundamentals", companyName)
	}

	// レポートの登録処理完了後、failed.json から該当のデータを削除する
	deleteFailedJsonItem(docID, dateKey, companyName)
	// レポートの登録処理完了後、invalid-summary.json から該当のデータを削除する

	fmt.Printf("「%s」のレポート(%s)の登録処理完了⭐️\n", companyName, docID)
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
	foundItems, err := QueryByName(dynamoClient, TableName, companyName, EDINETCode)
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
			TableName: aws.String(TableName),
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
		TableName: aws.String(TableName),
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
		TableName: aws.String(TableName),
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
		ErrMsg = "fundamental json.Marshal err: "
		RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
		return
	}
	// ファイル名
	fundamentalsFileName := fmt.Sprintf("%s-fundamentals-from-%s-to-%s.json", EDINETCode, fundamental.PeriodStart, fundamental.PeriodEnd)
	key := fmt.Sprintf("%s/Fundamentals/%s", EDINETCode, fundamentalsFileName)
	// ファイルの存在チェック
	existsFile, _ := S3Client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(BucketName),
		Key:    aws.String(key),
	})
	if existsFile == nil {
		_, err = S3Client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket:      aws.String(BucketName),
			Key:         aws.String(key),
			Body:        strings.NewReader(string(fundamentalBody)),
			ContentType: aws.String("application/json"),
		})
		if err != nil {
			// fmt.Println(err)
			ErrMsg = "fundamentals ファイルの S3 Put Object エラー: "
			RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
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

		fileType:                  BS もしくは PL
		body:                      ファイルボディ
		consolidatedBSMatches:     連結貸借対照表
		consolidatedBSIFRSMatches: 連結貸借対照表（IFRS）= 連結財政状態計算書
		soloBSMatches:             貸借対照表
		consolidatedPLMatches:     連結損益計算書
		consolidatedPLIFRSMatches: 連結損益計算書（IFRS）
		soloPLMatches:             損益計算書
	  BSFileNamePattern:         BSファイル名パターン
		PLFileNamePattern:         PLファイル名パターン
*/
func CreateHTML(docID string, dateKey string, fileType, consolidatedBSMatches, consolidatedBSIFRSMatches, soloBSMatches, consolidatedPLMatches, consolidatedPLIFRSMatches, soloPLMatches, BSFileNamePattern, PLFileNamePattern string) (*goquery.Document, error) {
	// エスケープ文字をデコード
	var unescapedStr string

	// BS の場合
	if fileType == "BS" {
		if consolidatedBSMatches == "" && soloBSMatches == "" {
			RegisterFailedJson(docID, dateKey, "parse 対象の貸借対照表データがありません")
			return nil, errors.New("parse 対象の貸借対照表データがありません")
		} else if consolidatedBSIFRSMatches != "" {
			// 優先順位1: 連結貸借対照表（IFRS）= 連結財政状態計算書
			fmt.Println("連結財政状態計算書 に該当する箇所があります⭐️")
			unescapedStr = html.UnescapeString(consolidatedBSIFRSMatches)
		} else if consolidatedBSMatches != "" {
			// 優先順位2: 連結貸借対照表
			unescapedStr = html.UnescapeString(consolidatedBSMatches)
		} else if soloBSMatches != "" {
			// 優先順位3: 単独貸借対照表
			unescapedStr = html.UnescapeString(soloBSMatches)
		}
	}

	// PL の場合
	if fileType == "PL" {
		if consolidatedPLMatches == "" && soloPLMatches == "" {
			RegisterFailedJson(docID, dateKey, "parse 対象の損益計算書データがありません")
			return nil, errors.New("parse 対象の損益計算書データがありません")
		} else if consolidatedPLIFRSMatches != "" {
			// 優先順位1: 連結損益計算書（IFRS）
			unescapedStr = html.UnescapeString(consolidatedPLIFRSMatches)
		} else if consolidatedPLMatches != "" {
			// 優先順位2: 連結損益計算書
			unescapedStr = html.UnescapeString(consolidatedPLMatches)
		} else if soloPLMatches != "" {
			// 優先順位3: 単独損益計算書
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
			ErrMsg = "HTML ローカルディレクトリ作成エラー: "
			RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
			fmt.Println("Error creating HTML directory:", err)
			return nil, err
		}
	}

	createFile, err := os.Create(filePath)
	if err != nil {
		ErrMsg = "HTML ローカルファイル作成エラー: "
		RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
		// fmt.Println("HTML create err: ", err)
		return nil, err
	}
	defer createFile.Close()

	_, err = createFile.WriteString(unescapedStr)
	if err != nil {
		ErrMsg = "HTML ローカルファイル書き込みエラー: "
		RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
		// fmt.Println("HTML write err: ", err)
		return nil, err
	}

	openFile, err := os.Open(filePath)
	if err != nil {
		ErrMsg = "HTML ローカルファイル書き込み後オープンエラー: "
		RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
		// fmt.Println("HTML open error: ", err)
		return nil, err
	}
	defer openFile.Close()

	// goqueryでHTMLをパース
	doc, err := goquery.NewDocumentFromReader(openFile)
	if err != nil {
		ErrMsg = "HTML goquery.NewDocumentFromReader error: "
		RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
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
		RegisterFailedJson(docID, dateKey, "パースする対象がありません")
		return nil, errors.New("パースする対象がありません")
	}

	var match string

	if consolidatedCFIFRSMattches != "" {
		// 優先順位1: 連結キャッシュ・フロー計算書 (IFRS)
		match = consolidatedCFIFRSMattches
	} else if consolidatedCFMattches != "" {
		// 優先順位2: 連結キャッシュ・フロー計算書
		match = consolidatedCFMattches
	} else if soloCFIFRSMattches != "" {
		// 優先順位3: 単独キャッシュ・フロー計算書（IFRS）
		match = soloCFIFRSMattches
	} else if soloCFMattches != "" {
		// 優先順位3: 単独キャッシュ・フロー計算書
		match = soloCFMattches
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
		ErrMsg = "CF HTML create err: "
		RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
		return nil, err
	}
	defer cfHTML.Close()

	// HTML ファイルに書き込み
	_, err = cfHTML.WriteString(unescapedMatch)
	if err != nil {
		ErrMsg = "CF HTML write err: "
		RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
		return nil, err
	}

	// HTML ファイルの読み込み
	cfHTMLFile, err := os.Open(cfHTMLFilePath)
	if err != nil {
		ErrMsg = "CF HTML open error: "
		RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
		return nil, err
	}
	defer cfHTMLFile.Close()

	// goqueryでHTMLをパース
	cfDoc, err := goquery.NewDocumentFromReader(cfHTMLFile)
	if err != nil {
		ErrMsg = "CF goquery.NewDocumentFromReader err: "
		RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
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
		ErrMsg = "Error creating JSON directory: "
		RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
		return "", err
	}

	jsonFile, err := os.Create(filePath)
	if err != nil {
		// fmt.Println(err)
		ErrMsg = "ローカル JSON ファイル作成エラー: "
		RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
		return "", err
	}
	defer jsonFile.Close()

	jsonBody, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		// fmt.Println(err)
		ErrMsg = "ローカル JSON MarshalIndent エラー: "
		RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
		return "", err
	}
	_, err = jsonFile.Write(jsonBody)
	if err != nil {
		// fmt.Println(err)
		ErrMsg = "ローカル JSON ファイル write エラー: "
		RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
		return "", err
	}
	return filePath, nil
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
func PutFileToS3(docID string, dateKey string, EDINETCode string, companyName string, fileNamePattern string, extension string, objectKeys []string, wg *sync.WaitGroup) {
	// 並列で処理する場合
	if Parallel == "true" {
		defer wg.Done()
	}

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
			ErrMsg = "ローカルファイル削除エラー: "
			RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
			return
		}
		// fmt.Printf("%s を削除しました\n", filePath)
	}()

	// S3 に ファイルを送信 (Key は aws configure で設定しておく)
	file, err := os.Open(filePath)
	if err != nil {
		ErrMsg = "S3 へのファイル送信時、ローカルファイル open エラー: "
		RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
		return
	}
	defer func() {
		err := file.Close()
		if err != nil {
			ErrMsg = "S3 へのファイル送信時、ローカルファイル close エラー: "
			RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
			return
		}
	}()

	splitByHyphen := strings.Split(fileName, "-")
	if len(splitByHyphen) >= 3 {
		reportType := splitByHyphen[2] // BS or PL or CF
		key := fmt.Sprintf("%s/%s/%s", EDINETCode, reportType, fileName)

		contentType, err := GetContentType(docID, dateKey, extension)
		if err != nil {
			ErrMsg = "ContentType 取得エラー: "
			RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
			return
		}

		// 登録したいファイル名 から BS-from-2000-01-01-to-2000-12-31 形式の文字列を見つける
		fromToRe := regexp.MustCompile(FromToPattern)
		fromToMatch := fromToRe.FindString(key)
		if fromToMatch != "" {
			splitBySlash := strings.Split(fromToMatch, "-")
			if len(splitBySlash) >= 1 {
				if len(objectKeys) > 0 {
					for _, objectKey := range objectKeys {
						if objectKey != key && strings.Contains(objectKey, fromToMatch) {
							// 同じ期間の古いファイルを S3 から削除
							_, err := S3Client.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
								Bucket: aws.String(BucketName),
								Key:    aws.String(objectKey),
							})
							if err != nil {
								fmt.Printf("%s/%s の削除に失敗しました❗️詳細: %v\n", BucketName, objectKey, err)
							} else {
								fmt.Printf("%s/%s を削除しました ⭐️\n", BucketName, objectKey)
							}
						}
					}
				}
			}
		}

		// 同名ファイルの存在チェック
		existsFile, err := CheckFileExists(S3Client, BucketName, key)
		if err != nil {
			fmt.Println("存在チェック時のエラー❗️: ", err)
		}
		// fmt.Printf("%s は登録済みですか❓ %v\n", key, existsFile)

		if existsFile {
			fmt.Printf("「%s」のレポート (ID: %s, ファイル名: %s) は S3 に登録済みです\n", companyName, docID, key)
		} else {
			// 同名ファイルがなければ登録
			_, err = S3Client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket:      aws.String(BucketName),
				Key:         aws.String(key),
				Body:        file,
				ContentType: aws.String(contentType),
			})
			if err != nil {
				ErrMsg = "S3 PutObject error: "
				RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
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
	RegisterFailedJson(docID, dateKey, "無効なファイル形式です")
	return "", errors.New("無効なファイル形式です")
}

func HandleRegisterJSON(docID string, dateKey string, EDINETCode string, companyName string, fileNamePattern string, summary interface{}, objectKeys []string, wg *sync.WaitGroup) {
	_, err := CreateJSON(docID, dateKey, fileNamePattern, summary)
	if err != nil {
		ErrMsg = "CF JSON ファイル作成エラー: "
		RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
		return
	}
	PutFileToS3(docID, dateKey, EDINETCode, companyName, fileNamePattern, "json", objectKeys, wg)
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
func RegisterFailedJson(docID string, dateKey string, ErrMsg string) {
	fmt.Println(ErrMsg)
	// 取得から更新までをロック
	Mu.Lock()
	// 更新まで終わったらロック解除
	defer Mu.Unlock()

	// json ファイルの取得
	openFile, _ := os.Open(FailedJSONFile)
	fmt.Println("JSON Open File ⭐️: ", openFile)
	defer openFile.Close()
	if openFile == nil {
		// ファイルがない場合は作成する
		_, err := os.Create(FailedJSONFile)
		if err != nil {
			fmt.Println("failed json create error: ", err)
			// RegisterFailedJson(docID, dateKey, err.Error())
		}
	}
	// fmt.Println("failed.json file: ", file)

	// 中身を取得
	body, err := io.ReadAll(openFile)
	if err != nil {
		fmt.Println("failed json read error: ", err)
		// RegisterFailedJson(docID, dateKey, err.Error())
	}
	err = json.Unmarshal(body, &FailedReports)
	if err != nil {
		fmt.Println("RegisterFailedJson")
		fmt.Println("failed json unmarshal error: ", err)
		// RegisterFailedJson(docID, dateKey, err.Error())
	}
	// 配列の中に自分がいるか確認
	alreadyFailed := false
	for _, report := range FailedReports {
		if report.DocID == docID {
			alreadyFailed = true
		}
	}
	// 未登録であれば登録
	if !alreadyFailed {
		failedReport := FailedReport{
			DocID:        docID,
			RegisterDate: dateKey,
			ErrorMsg:     ErrMsg,
		}
		FailedReports = append(FailedReports, failedReport)
		jsonBody, err := json.MarshalIndent(FailedReports, "", "  ")
		if err != nil {
			fmt.Println("json MarshalIndent when trying to write failed json error: ", err)
			// RegisterFailedJson(docID, dateKey, err.Error())
		}
		// json を書き出す
		err = os.WriteFile(FailedJSONFile, jsonBody, 0666)
		if err != nil {
			fmt.Println("failed json write error: ", err)
			// RegisterFailedJson(docID, dateKey, err.Error())
		}
	}
}

// TODO: S3 のファイルを操作する
func deleteFailedJsonItem(docID string, dateKey string, companyName string) {
	Mu.Lock()
	// 更新まで終わったらロック解除
	defer Mu.Unlock()
	// json ファイルの取得
	openFile, _ := os.Open(FailedJSONFile)
	defer openFile.Close()
	if openFile == nil {
		// ファイルがない場合は作成する
		_, err := os.Create(FailedJSONFile)
		if err != nil {
			fmt.Println("failed json create error: ", err)
			// RegisterFailedJson(docID, dateKey, err.Error())
		}
	}
	// fmt.Println("failed.json file: ", file)

	// 中身を取得
	body, err := io.ReadAll(openFile)
	if err != nil {
		fmt.Println("failed json read error: ", err)
		// RegisterFailedJson(docID, dateKey, err.Error())
	}
	err = json.Unmarshal(body, &FailedReports)
	if err != nil {
		fmt.Println("deleteFailedJsonItem")
		fmt.Println("failed json unmarshal error: ", err)
		// RegisterFailedJson(docID, dateKey, err.Error())
	}
	// 自分を取り除いたスライスを作成
	var newFailedReports []FailedReport

	alreadyFailed := false
	for _, report := range FailedReports {
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
			// RegisterFailedJson(docID, dateKey, err.Error())
		}
		// json を S3 に書き出す
		err = PutJSONObject(S3Client, BucketName, "failed.json", jsonBody)
		if err != nil {
			fmt.Println("failed json put object error: ", err)
		}
		// err = os.WriteFile(FailedJSONFile, jsonBody, 0666)
		// if err != nil {
		// 	fmt.Println("failed json write error: ", err)
		// 	// RegisterFailedJson(docID, dateKey, err.Error())
		// }
	}
}

/*
無効なサマリーだった場合にjsonファイルに記録する
*/
// TODO: S3 のファイルを操作する
func RegisterInvalidSummaryJson(docID string, dateKey string, summaryType string, companyName string) {
	// 取得から更新までをロック
	Mu.Lock()
	// 更新まで終わったらロック解除
	defer Mu.Unlock()

	// json ファイルの取得
	openFile, _ := os.Open(InvalidSummaryJSONFile)
	defer openFile.Close()
	if openFile == nil {
		// ファイルがない場合は作成する
		_, err := os.Create(InvalidSummaryJSONFile)
		if err != nil {
			fmt.Println("failed json create error: ", err)
			// RegisterFailedJson(docID, dateKey, err.Error())
		}
	}
	// fmt.Println("failed.json file: ", file)

	// 中身を取得
	body, err := io.ReadAll(openFile)
	if err != nil {
		fmt.Println("failed json read error: ", err)
		// RegisterFailedJson(docID, dateKey, err.Error())
	}
	err = json.Unmarshal(body, &InvalidSummaries)
	if err != nil {
		fmt.Println("registerInvalidSummaryJson")
		fmt.Println("failed json unmarshal error: ", err)
		// RegisterFailedJson(docID, dateKey, err.Error())
	}
	// 配列の中に自分がいるか確認
	alreadyFailed := false
	for _, report := range InvalidSummaries {
		if report.DocID == docID {
			alreadyFailed = true
		}
	}
	// 未登録であれば登録
	if !alreadyFailed {
		invalidSummary := InvalidSummary{
			DocID:        docID,
			RegisterDate: dateKey,
			ErrorMsg:     ErrMsg,
			SummaryType:  summaryType,
			CompanyName:  companyName,
		}
		InvalidSummaries = append(InvalidSummaries, invalidSummary)
		jsonBody, err := json.MarshalIndent(InvalidSummaries, "", "  ")
		if err != nil {
			fmt.Println("json MarshalIndent when trying to write invalid summary error: ", err)
			// RegisterFailedJson(docID, dateKey, err.Error())
		}
		// json を S3 に書き出す
		err = PutJSONObject(S3Client, BucketName, "invalid-summary.json", jsonBody)
		if err != nil {
			fmt.Println("invalid summary json put object error: ", err)
		}

		// err = os.WriteFile(InvalidSummaryJSONFile, jsonBody, 0666)
		// if err != nil {
		// 	fmt.Println("invalid summary write error: ", err)
		// 	// RegisterFailedJson(docID, dateKey, err.Error())
		// }
	}
}

// TODO: S3 のファイルを操作する
func deleteInvalidSummaryJsonItem(docID string, dateKey string, summaryType string, companyName string) {
	// 取得から更新までをロック
	Mu.Lock()
	// 更新まで終わったらロック解除
	defer Mu.Unlock()

	// json ファイルの取得
	openFile, _ := os.Open(InvalidSummaryJSONFile)
	defer openFile.Close()
	if openFile == nil {
		// ファイルがない場合は作成する
		_, err := os.Create(InvalidSummaryJSONFile)
		if err != nil {
			fmt.Println("failed json create error: ", err)
			// RegisterFailedJson(docID, dateKey, err.Error())
		}
	}
	// fmt.Println("failed.json file: ", file)

	// 中身を取得
	body, err := io.ReadAll(openFile)
	if err != nil {
		fmt.Println("failed json read error: ", err)
		// RegisterFailedJson(docID, dateKey, err.Error())
	}
	err = json.Unmarshal(body, &InvalidSummaries)
	if err != nil {
		fmt.Println("deleteInvalidSummaryJsonItem")
		fmt.Println("failed json unmarshal error: ", err)
		// RegisterFailedJson(docID, dateKey, err.Error())
	}

	// 自分を取り除いたスライスを作成
	var newInvalidSummaries []InvalidSummary

	// 配列の中に自分がいるか確認
	alreadyFailed := false
	for _, report := range InvalidSummaries {
		if report.DocID == docID {
			alreadyFailed = true
		} else {
			newInvalidSummaries = append(newInvalidSummaries, report)
		}
	}
	// invalid-summary.json に登録されていれば json を登録し直す
	if alreadyFailed {
		fmt.Printf("invalid-summary.json から「%s」のレポート (ID: %s, Type: %s) を削除したもので書き換えます❗️\n", companyName, docID, summaryType)

		jsonBody, err := json.MarshalIndent(newInvalidSummaries, "", "  ")
		if err != nil {
			fmt.Println("json MarshalIndent when trying to write invalid summary error: ", err)
			// RegisterFailedJson(docID, dateKey, err.Error())
		}
		// json S3 にを書き出す
		err = PutJSONObject(S3Client, BucketName, "invalid-summary.json", jsonBody)
		if err != nil {
			fmt.Println("invalid summary json put object error: ", err)
		}

		// err = os.WriteFile(InvalidSummaryJSONFile, jsonBody, 0666)
		// if err != nil {
		// 	fmt.Println("invalid summary write error: ", err)
		// 	// RegisterFailedJson(docID, dateKey, err.Error())
		// }
	}
}

func PutXBRLtoS3(docID string, dateKey string, key string, body []byte) {
	// ファイルの存在チェック
	existsFile, _ := S3Client.HeadObject(context.TODO(), &s3.HeadObjectInput{
		Bucket: aws.String(EDINETBucketName),
		Key:    aws.String(key),
	})
	if existsFile == nil {
		_, err := S3Client.PutObject(context.TODO(), &s3.PutObjectInput{
			Bucket:      aws.String(EDINETBucketName),
			Key:         aws.String(key),
			Body:        strings.NewReader(string(body)),
			ContentType: aws.String("application/xml"),
		})
		if err != nil {
			ErrMsg = "S3 への XBRL ファイル送信エラー: "
			RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
			return
		}
		fmt.Printf("xbrlファイル(%s)をS3に送信しました⭐️\n", key)
	}
}

func GetTitleValue(docID string, dateKey string, titleName string, previousText string, currentText string) (TitleValue, error) {
	previousIntValue, err := ConvertTextValue2IntValue(previousText)
	if err != nil {
		if err.Error() != EmptyStrConvErr {
			ErrMsg = "ConvertTextValue2IntValue (PL previous) エラー: "
			RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
		}
		return TitleValue{}, err
	}
	currentIntValue, err := ConvertTextValue2IntValue(currentText)
	if err != nil {
		if err.Error() != EmptyStrConvErr {
			ErrMsg = "ConvertTextValue2IntValue (PL previous) エラー: "
			RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
		}
		return TitleValue{}, err
	}
	return TitleValue{
		Previous: previousIntValue,
		Current:  currentIntValue,
	}, nil
}

func UpdateEverySummary(doc *goquery.Document, docID string, dateKey string, summaryType string, summary *Summary, plSummary *PLSummary, cfSummary *CFSummary, fundamental *Fundamental) {
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

		var titleValue TitleValue
		var titleName string
		var err error
		if len(titleTexts) >= 1 {
			titleName = titleTexts[0]
		}
		if len(titleTexts) >= 4 {
			titleValue, err = GetTitleValue(docID, dateKey, titleName, titleTexts[2], titleTexts[3])
			if err != nil {
				return
			}
		} else if len(titleTexts) >= 3 {
			titleValue, err = GetTitleValue(docID, dateKey, titleName, titleTexts[1], titleTexts[2])
			if err != nil {
				return
			}
		}

		if summaryType == "bs" {

			if titleName == "流動資産合計" {
				summary.CurrentAssets.Previous = titleValue.Previous
				summary.CurrentAssets.Current = titleValue.Current
			}
			if titleName == "有形固定資産合計" {
				summary.TangibleAssets.Previous = titleValue.Previous
				summary.TangibleAssets.Current = titleValue.Current
			}
			if titleName == "無形固定資産合計" {
				summary.IntangibleAssets.Previous = titleValue.Previous
				summary.IntangibleAssets.Current = titleValue.Current
			}
			if titleName == "投資その他の資産合計" {
				summary.InvestmentsAndOtherAssets.Previous = titleValue.Previous
				summary.InvestmentsAndOtherAssets.Current = titleValue.Current
			}
			if titleName == "流動負債合計" {
				summary.CurrentLiabilities.Previous = titleValue.Previous
				summary.CurrentLiabilities.Current = titleValue.Current
			}
			if titleName == "固定負債合計" {
				summary.FixedLiabilities.Previous = titleValue.Previous
				summary.FixedLiabilities.Current = titleValue.Current
			}
			if titleName == "純資産合計" {
				summary.NetAssets.Previous = titleValue.Previous
				summary.NetAssets.Current = titleValue.Current
				// fundamental
				fundamental.NetAssets = titleValue.Current
			}
			if titleName == "負債合計" {
				// fundamental
				fundamental.Liabilities = titleValue.Current
			}

			if len(splitTdTexts) == 1 && titleTexts != nil && strings.Contains(titleTexts[0], "単位：") && summary.UnitString == "" {
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
		} else if summaryType == "pl" {
			// 営業収益合計の設定が終わったかどうか管理するフラグ
			isOperatingRevenueDone := false
			// 営業費用合計の設定が終わったかどうか管理するフラグ
			isOperatingCostDone := false

			if strings.Contains(titleName, "売上原価") {
				plSummary.CostOfGoodsSold.Previous = titleValue.Previous
				plSummary.CostOfGoodsSold.Current = titleValue.Current
			}
			if strings.Contains(titleName, "販売費及び一般管理費") {
				plSummary.SGAndA.Previous = titleValue.Previous
				plSummary.SGAndA.Current = titleValue.Current
			}
			if strings.Contains(titleName, "売上高") {
				plSummary.Sales.Previous = titleValue.Previous
				plSummary.Sales.Current = titleValue.Current
				// fundamental
				fundamental.Sales = titleValue.Current
			}
			if strings.Contains(titleName, "営業利益") {
				plSummary.OperatingProfit.Previous = titleValue.Previous
				plSummary.OperatingProfit.Current = titleValue.Current
				// fundamental
				fundamental.OperatingProfit = titleValue.Current
			}
			if titleName == "営業損失（△）" {
				// fmt.Println("営業損失とだけ書いてあります")
				if plSummary.OperatingProfit.Previous == 0 {
					plSummary.OperatingProfit.Previous = titleValue.Previous
				}
				if plSummary.OperatingProfit.Current == 0 {
					plSummary.OperatingProfit.Current = titleValue.Current
				}
				// fundamental
				if fundamental.OperatingProfit == 0 {
					fundamental.OperatingProfit = titleValue.Current
				}
			}
			// 営業収益の後に営業収益合計がある場合は上書き
			if (strings.Contains(titleName, "営業収益") || strings.Contains(titleName, "売上収益")) && !isOperatingRevenueDone {
				plSummary.HasOperatingRevenue = true
				plSummary.OperatingRevenue.Previous = titleValue.Previous
				plSummary.OperatingRevenue.Current = titleValue.Current
				// fundamental
				fundamental.HasOperatingRevenue = true
				fundamental.OperatingRevenue = titleValue.Current
				if titleName == "営業収益合計" {
					isOperatingRevenueDone = true
				}
			}
			// 営業費用の後に営業費用合計がある場合は上書き
			if strings.Contains(titleName, "営業費用") && !isOperatingCostDone {
				plSummary.HasOperatingCost = true
				plSummary.OperatingCost.Previous = titleValue.Previous
				plSummary.OperatingCost.Current = titleValue.Current
				// fundamental
				fundamental.HasOperatingCost = true
				fundamental.OperatingCost = titleValue.Current
				if titleName == "営業費用合計" {
					isOperatingCostDone = true
				}
			}

			if len(splitTdTexts) == 1 && titleTexts != nil && strings.Contains(titleTexts[0], "単位：") && plSummary.UnitString == "" {
				baseStr := splitTdTexts[0]
				baseStr = strings.ReplaceAll(baseStr, "(", "")
				baseStr = strings.ReplaceAll(baseStr, ")", "")
				splitUnitStrs := strings.Split(baseStr, "：")
				if len(splitUnitStrs) >= 2 {
					plSummary.UnitString = splitUnitStrs[1]
				}
			}
		} else if summaryType == "cf" {
			if strings.Contains(titleName, "営業活動による") {
				// TODO: テスト後消す
				// fmt.Println("==================")
				// fmt.Println("項目名⭐️: ", titleName)
				// fmt.Println("今期のテキスト⭐️: ", currentText)
				cfSummary.OperatingCF.Previous = titleValue.Previous
				cfSummary.OperatingCF.Current = titleValue.Current
			}
			if strings.Contains(titleName, "投資活動による") {
				cfSummary.InvestingCF.Previous = titleValue.Previous
				cfSummary.InvestingCF.Current = titleValue.Current
			}
			if strings.Contains(titleName, "財務活動による") {
				cfSummary.FinancingCF.Previous = titleValue.Previous
				cfSummary.FinancingCF.Current = titleValue.Current
			}
			if strings.Contains(titleName, "期首残高") {
				cfSummary.StartCash.Previous = titleValue.Previous
				cfSummary.StartCash.Current = titleValue.Current
			}
			if strings.Contains(titleName, "期末残高") {
				cfSummary.EndCash.Previous = titleValue.Previous
				cfSummary.EndCash.Current = titleValue.Current
			}

			if len(splitTdTexts) == 1 && titleTexts != nil && strings.Contains(titleTexts[0], "単位：") && cfSummary.UnitString == "" {
				formatUnitStr := FormatUnitStr(splitTdTexts[0])
				if formatUnitStr != "" {
					cfSummary.UnitString = formatUnitStr
				}
			}
		}
	})
}

func CreateFailedFiles() {
	fmt.Println("失敗用ファイル作成開始⭐️")
	err := os.WriteFile(InvalidSummaryJSONFile, []byte("[]"), 0777)
	if err != nil {
		log.Fatalf("%s の初期作成に失敗しました。\n", InvalidSummaryJSONFile)
	}
	err = os.WriteFile(FailedJSONFile, []byte("[]"), 0777)
	if err != nil {
		log.Fatalf("%s の初期作成に失敗しました。\n", FailedJSONFile)
	}
	fmt.Println("失敗したデータ格納ファイルの作成が完了しました⭐️")
}

func PutOriginalHTMLToS3(docID string, dateKey string, fileKey string, body string) {
	// ファイルキーから .xbrl の箇所を取得する
	HTMLFileKey := ConvertExtensionFromXBRLToHTML(fileKey)
	if HTMLFileKey != "" {
		unescapedStr := html.UnescapeString(body)
		// 特定のエンティティをさらに手動でデコード
		unescapedStr = strings.ReplaceAll(unescapedStr, "&apos;", "'")

		// HTMLデータを加工
		unescapedStr = FormatHtmlTable(unescapedStr)

		// 同名ファイルの存在チェック
		existsFile, err := CheckFileExists(S3Client, EDINETBucketName, HTMLFileKey)
		if err != nil {
			fmt.Println("存在チェック時のエラー❗️: ", err)
		}
		// fmt.Printf("%s は登録済みですか❓ %v\n", key, existsFile)

		// 同名ファイルがなければ登録
		if !existsFile {
			_, err = S3Client.PutObject(context.TODO(), &s3.PutObjectInput{
				Bucket:      aws.String(EDINETBucketName),
				Key:         aws.String(HTMLFileKey),
				Body:        strings.NewReader(string(unescapedStr)),
				ContentType: aws.String("text/html"),
			})
			if err != nil {
				ErrMsg = "S3 Original HTML PutObject error: "
				RegisterFailedJson(docID, dateKey, ErrMsg+err.Error())
				return
			}
			uploadDoneMsg := fmt.Sprintf("オリジナルHTML (%s) を登録しました ⭕️ ", HTMLFileKey)
			fmt.Println(uploadDoneMsg)
		}
	}
}

func ConvertExtensionFromXBRLToHTML(fileKey string) string {
	XBRLExtensionRe := regexp.MustCompile(XBRLExtensionPattern)
	XBRLExtensionMatch := XBRLExtensionRe.FindString(fileKey)
	if XBRLExtensionMatch != "" {
		// .xbrl を .html に変換
		HTMLFileKey := strings.ReplaceAll(fileKey, ".xbrl", ".html")
		return HTMLFileKey
	}
	// 変換できなければファイルキーをそのまま返す
	return ""
}
