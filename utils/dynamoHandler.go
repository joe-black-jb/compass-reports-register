package utils

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func UpdateSecCode(dynamoClient *dynamodb.Client, EDINETCode string, securityCode string) error {
	fmt.Printf("証券コードパラメータ: %s ⚾️\n", securityCode)
	trimmedSecCode := strings.TrimSpace(securityCode)
	if trimmedSecCode != "" {
		queryInput := &dynamodb.QueryInput{
			TableName: aws.String("compass_companies"),
			IndexName: aws.String("edinetCode-index"),
			KeyConditionExpression: aws.String("#k = :v"),
			ExpressionAttributeNames: map[string]string{
				"#k": "edinetCode",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":v": &types.AttributeValueMemberS{Value: EDINETCode},
			},
		}
		// クエリを実行
		queryOutput, err := dynamoClient.Query(context.TODO(), queryInput)
		if err != nil {
			fmt.Println("query error: ", err)
			return err
		}

		if len(queryOutput.Items) > 0 {
			var company Company
			// primaryKey := queryOutput.Items[0]["PrimaryKeyAttributeName"].(*types.AttributeValueMemberS).Value
			// fmt.Println("プライマリキー: ", primaryKey)
			item := queryOutput.Items[0]
			err := attributevalue.UnmarshalMap(item, &company)
			if err != nil {
				fmt.Println("MarshalMap err: ", err)
				return err
			}
			fmt.Println("company: ", company)

			if company.SecurityCode == "" {
				loc, err := time.LoadLocation("Asia/Tokyo")
				if err != nil {
					return err
				}
				// 更新するカラムとその値の指定
				updateInput := &dynamodb.UpdateItemInput{
					TableName: aws.String(TableName),
					Key: map[string]types.AttributeValue{
						"id": &types.AttributeValueMemberS{Value: company.ID},
					},
					UpdateExpression: aws.String("SET #secCode = :secCode, #updatedAt = :updatedAt"),
					ExpressionAttributeNames: map[string]string{
						"#secCode": "securityCode", // "securityCode" カラムを指定
						"#updatedAt": "updatedAt",    // "updateAt" カラムを指定
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":secCode": &types.AttributeValueMemberS{Value: securityCode},
						":updatedAt": &types.AttributeValueMemberS{Value: time.Now().In(loc).Format(time.RFC3339)}, // 現在の日時を設定
					},
					ReturnValues: types.ReturnValueUpdatedNew, // 更新後の新しい値を返す
				}

				// 更新の実行
				_, err = dynamoClient.UpdateItem(context.TODO(), updateInput)
				if err != nil {
					return err
				}
				fmt.Printf("「%s」の証券コード (%s) をDBに設定しました⚾️\n", company.Name, securityCode)
				return nil
			}
		}
	}
	return nil
}