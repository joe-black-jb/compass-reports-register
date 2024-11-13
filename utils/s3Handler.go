package utils

import (
	"context"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go/aws"
)

func ListS3Objects(s3Client *s3.Client, bucketName string, key string) *s3.ListObjectsV2Output {
	output, _ := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(key),
	})
	return output
}

func GetS3Object(s3Client *s3.Client, bucketName string, key string) (*s3.GetObjectOutput, error) {
	output, err := s3Client.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	return output, nil
}

func CheckExistsS3Key(s3Client *s3.Client, bucketName string, key string) (bool, error) {
	// ファイルの存在チェック
	output, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucketName),
		Prefix:  aws.String(key),
		MaxKeys: aws.Int32(1),
	})
	// fmt.Printf("%s/%s の存在チェック結果: %v\n", bucketName, key, existsFile)
	if err != nil {
		return false, err
	}

	return len(output.Contents) > 0, nil
}

func ConvertTextValue2IntValue(text string) (int, error) {
	isMinus := false
	// fmt.Println("==========================")
	// fmt.Println("元text: ", text)

	// , を削除
	text = strings.ReplaceAll(text, ",", "")
	// ※1 などを削除
	asteriskAndHalfWidthNums := AsteriskAndHalfWidthNumRe.FindAllString(text, -1)
	for _, asteriskAndHalfWidthNum := range asteriskAndHalfWidthNums {
		text = strings.ReplaceAll(text, asteriskAndHalfWidthNum, "")
	}
	// マイナスチェック
	if strings.Contains(text, "△") {
		isMinus = true
	}
	// 数字部分のみ抜き出す
	text = OnlyNumRe.FindString(text)
	// スペースを削除
	text = strings.TrimSpace(text)
	// マイナスの場合、 - を先頭に追加する
	if isMinus {
		// previousMatch = "-" + previousMatch
		text = "-" + text
	}
	intValue, err := strconv.Atoi(text)
	if err != nil {
		// fmt.Println("strconv.Atoi error text: ", text)
		return 0, err
	}
	return intValue, nil
}

func QueryByName(svc *dynamodb.Client, tableName string, companyName string, edinetCode string) ([]map[string]types.AttributeValue, error) {
	input := &dynamodb.QueryInput{
		TableName:              aws.String(tableName),
		IndexName:              aws.String("CompanyNameIndex"), // GSIを指定
		KeyConditionExpression: aws.String("#n = :name AND #e = :edinetCode"),
		ExpressionAttributeNames: map[string]string{
			"#n": "name",       // `name`をエイリアス
			"#e": "edinetCode", // `edinetCode`をエイリアス
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":name":       &types.AttributeValueMemberS{Value: companyName},
			":edinetCode": &types.AttributeValueMemberS{Value: edinetCode},
		},
	}

	// クエリの実行
	result, err := svc.Query(context.TODO(), input)
	if err != nil {
		return nil, err
	}

	return result.Items, nil
}

func PutJSONObject(s3Client *s3.Client, bucketName string, key string, body []byte) error {
	_, err := s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(key),
		Body:        strings.NewReader(string(body)),
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		return err
	}
	return nil
}

/*
正常系
	10,897,603
異常系
	※1 10,897,603
	※1,※2 10,897,603
*/
// 数字のみのパターン (※１ などを除外するためのパターン)
// 数字の前に※がないことを条件に加える
var OnlyNumRe *regexp.Regexp = regexp.MustCompile(`\d+`)

// var OnlyNumRe *regexp.Regexp = regexp.MustCompile(`(?!※)\d+`)

// ※1 などを除外するためのパターン
var AsteriskAndHalfWidthNumRe *regexp.Regexp = regexp.MustCompile(`※\d+`)
