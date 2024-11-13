package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/joe-black-jb/compass-reports-register/utils"
)

func handler(ctx context.Context) {
	start := time.Now()

	if utils.TableName == "" {
		log.Fatal("テーブル名が設定されていません")
	}

	reports, err := utils.GetReports()
	fmt.Println("len(reports): ", len(reports))
	if err != nil {
		fmt.Println(err)
		return
	}

	// invalid-summary.json と failed.json を /tmp ディレクトリに書き込む
	// invalid-summary.json
	invalidSummaryKey := "invalid-summary.json"
	invalidOutput, err := utils.GetS3Object(utils.S3Client, utils.BucketName, invalidSummaryKey)
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
	failedOutput, err := utils.GetS3Object(utils.S3Client, utils.BucketName, failedKey)
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
		if utils.Parallel == "true" {
			wg.Add(1)
		}

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
		fundamental := utils.Fundamental{
			CompanyName:     companyName,
			PeriodStart:     periodStart,
			PeriodEnd:       periodEnd,
			Sales:           0,
			OperatingProfit: 0,
			Liabilities:     0,
			NetAssets:       0,
		}
		if utils.Parallel == "true" {
			// 並列で処理する場合
			go utils.RegisterReport(utils.DynamoClient, EDINETCode, docID, report.DateKey, companyName, periodStart, periodEnd, &fundamental, &wg)
		} else {
			// 直列で処理する場合
			utils.RegisterReport(utils.DynamoClient, EDINETCode, docID, report.DateKey, companyName, periodStart, periodEnd, &fundamental, &wg)
		}
	}
	// 並列で処理する場合
	if utils.Parallel == "true" {
		wg.Wait()
	}

	fmt.Println("All processes done ⭐️")
	fmt.Println("APIを叩いた回数(概算): ", utils.ApiTimes)
	fmt.Println("所要時間: ", time.Since(start))
}

func main() {
	fmt.Println("main start")
	if utils.Env == "local" {
		fmt.Println("ローカルです⭐️")
		handler(context.TODO())
		fmt.Println("ローカルでの処理が完了しました⭐️")
	} else if utils.Env == "production" {
		lambda.Start(handler)
	}
}
