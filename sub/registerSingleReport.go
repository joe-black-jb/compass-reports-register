//go:build ignore
// +build ignore

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/joe-black-jb/compass-reports-register/utils"
	"github.com/joho/godotenv"
)

func main(){
	fmt.Println("特定の資料のみ登録します⭐️")

	env := os.Getenv("ENV")
	fmt.Println("環境: ", env)

	if env == "local" {
		err := godotenv.Load()
		if err != nil {
			log.Fatal("Error loading .env file err: ", err)
			return
		}
	}

	cfg, cfgErr := config.LoadDefaultConfig(context.TODO())
	if cfgErr != nil {
		fmt.Println("Load default config error: %v", cfgErr)
		return
	}
	dynamoClient := dynamodb.NewFromConfig(cfg)

	// 楽天グループ
	companyName := os.Getenv("SINGLE_COMPANY_NAME")
	singleEDINETCode := os.Getenv("SINGLE_EDINET_CODE")
	singleDocID := os.Getenv("SINGLE_DOC_ID")
	singleDateKey := os.Getenv("SINGLE_DATE_KEY")
	periodStart := os.Getenv("SINGLE_PERIOD_START")
	periodEnd := os.Getenv("SINGLE_PERIOD_END")
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
	var singleWg sync.WaitGroup
	utils.RegisterReport(dynamoClient, singleEDINETCode, singleDocID, singleDateKey, companyName, periodStart, periodEnd, &fundamental, &singleWg)
}

