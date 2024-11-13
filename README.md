# 特定の資料のみ処理する場合

1. EDINET で該当企業が資料を登録した日付を調べる

2. EDINET の資料一覧取得 API で調べた日付を指定し、該当資料がレスポンスに含まれているか確認する

3. ソースコードの日付を変更する

4. 環境変数を設定する

   ```sh
   REGISTER_SINGLE_REPORT=true
   GET_XBRL_FROM_S3=true
   XBRL_FILE_NAME={取得したいXBRLファイル名}
   PARALLEL=false
   ```

5. 登録済みのファイルを削除する

- edinet-reports-bucket/{YYYYmmdd}/{DocID}
- compass-reports-bucket/{EDINET コード} 配下全て
  ※ DynamoDB で調べる

6. バッチを実行する

   ```sh
   make xbrl
   ```
