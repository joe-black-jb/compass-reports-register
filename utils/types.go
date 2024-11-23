package utils

import (
	"encoding/xml"
	"time"

	"gorm.io/gorm"
)

type User struct {
	gorm.Model
	Name     string
	Email    string `gorm:"unique"`
	Password []byte
	Admin    bool
}

type Company struct {
	ID         string    `json:"id" dynamodbav:"id"`
	CreatedAt  time.Time `json:"createdAt" dynamodbav:"createdAt"`
	UpdatedAt  time.Time `json:"updatedAt" dynamodbav:"updatedAt"`
	Name       string    `json:"name" dynamodbav:"name"`
	EDINETCode string    `json:"edinetCode" dynamodbav:"edinetCode"`
	SecurityCode string    `json:"securityCode" dynamodbav:"securityCode"`
	BS         int       `json:"bs" dynamodbav:"bs"`
	PL         int       `json:"pl" dynamodbav:"pl"`
}

type Title struct {
	gorm.Model
	Name          string `gorm:"type:varchar(255);uniqueIndex:name_company_unique"`
	Category      string
	CompanyID     int `gorm:"type:varchar(255);uniqueIndex:name_company_unique"`
	Depth         int
	HasValue      bool
	StatementType int
	Order         int `json:"order" gorm:"default:null"`
	FiscalYear    int
	Value         string
	ParentTitleId int             `json:"parent_title_id" gorm:"default:null"`
	Companies     []*Company      `gorm:"many2many:company_titles;"`
	CompanyTitles []*CompanyTitle `gorm:"foreignKey:TitleID"`
}

type CompanyTitle struct {
	gorm.Model
	CompanyID int `gorm:"primaryKey"`
	TitleID   int `gorm:"primaryKey"`
	Value     string
	Company   Company `gorm:"foreignKey:CompanyID"`
	Title     Title   `gorm:"foreignKey:TitleID"`
}

type UpdateCompanyTitleParams struct {
	Name  string
	Value string
}

type CreateTitleBody struct {
	Name          *string
	Category      *string
	CompanyID     *int
	Depth         *int
	HasValue      *bool
	StatementType *int
	Order         *int
	FiscalYear    *int
	Value         *string
	ParentTitleId *int
}

type Error struct {
	Status  int
	Message string
}

type Ok struct {
	Status  int
	Message string
}

type Credentials struct {
	Email    string
	Password string
}

type RegisterUserBody struct {
	Name     *string
	Email    *string
	Password *string
}

type Login struct {
	Username string
	Token    string
}

type ReportData struct {
	FileName string `json:"file_name"`
	Data     string `json:"data"`
}

// <link:schemaRef> 要素
type SchemaRef struct {
	Href string `xml:"xlink:href,attr"`
	Type string `xml:"xlink:type,attr"`
}

// <xbrli:identifier> 要素
type Identifier struct {
	Scheme string `xml:"scheme,attr"`
	Value  string `xml:",chardata"`
}

// <xbrli:entity> 要素
type Entity struct {
	Identifier Identifier `xml:"identifier"`
}

// <xbrli:period> 要素
type Period struct {
	Instant string `xml:"instant"`
}

// <xbrli:context> 要素
type Context struct {
	ID     string `xml:"id,attr"`
	Entity Entity `xml:"entity"`
	Period Period `xml:"period"`
}

// <jppfs_cor:MoneyHeldInTrustCAFND> タグの構造体
type MoneyHeldInTrust struct {
	ContextRef string `xml:"contextRef,attr"`
	Decimals   string `xml:"decimals,attr"`
	UnitRef    string `xml:"unitRef,attr"`
	Value      string `xml:",chardata"`
}

// 貸借対照表の行（Assets or Liabilities）
type BalanceSheetItem struct {
	Description string `xml:"td>p>span"`              // 項目名
	AmountYear1 string `xml:"td:nth-child(2)>p>span"` // 特定28期の金額
	AmountYear2 string `xml:"td:nth-child(3)>p>span"` // 特定29期の金額
}

// 貸借対照表の構造体
type BalanceSheet struct {
	Title string             `xml:"p>span"`   // 貸借対照表のタイトル
	Unit  string             `xml:"caption"`  // 単位
	Items []BalanceSheetItem `xml:"tbody>tr"` // 資産、負債の各項目
}

// XML全体のルート構造体
type XBRL struct {
	XMLName                                                                 xml.Name         `xml:"xbrl"`
	SchemaRef                                                               SchemaRef        `xml:"schemaRef"`
	Contexts                                                                []Context        `xml:"context"`
	MoneyHeldInTrust                                                        MoneyHeldInTrust `xml:"MoneyHeldInTrustCAFND"`
	BalanceSheetTextBlock                                                   BalanceSheet     `xml:"BalanceSheetTextBlock"`
	NotesFinancialInformationOfInvestmentTrustManagementCompanyEtcTextBlock BalanceSheet     `xml:"NotesFinancialInformationOfInvestmentTrustManagementCompanyEtcTextBlock"`
}

// 項目ごとの値
type TitleValue struct {
	Previous int `json:"previous"`
	Current  int `json:"current"`
}

type MyError interface {
	Error() string
}

type Param struct {
	Date string `json:"date"`
	Type string `json:"type"`
}

type ResultCount struct {
	Count int `json:"count"`
}

type Meta struct {
	Title           string      `json:"title"`
	Parameter       Param       `json:"parameter"`
	ResultSet       ResultCount `json:"resultset"`
	ProcessDateTime string      `json:"processDateTime"`
	Status          string      `json:"status"`
	Message         string      `json:"message"`
}

type Result struct {
	DateKey              string `json:"dateKey"` // EDINETの仕様書にはない値
	SeqNumber            int    `json:"seqNumber"`
	DocId                string `json:"docId"`
	EdinetCode           string `json:"edinetCode"`
	SecCode              string `json:"secCode"`
	JCN                  string `json:"JCN"`
	FilerName            string `json:"filerName"` // 企業名
	FundCode             string `json:"fundCode"`
	OrdinanceCode        string `json:"ordinanceCode"`
	FormCode             string `json:"formCode"`
	DocTypeCode          string `json:"docTypeCode"`
	PeriodStart          string `json:"periodStart"`
	PeriodEnd            string `json:"periodEnd"`
	SubmitDateTime       string `json:"submitDateTime"` // Date にしたほうがいいかも
	DocDescription       string `json:"docDescription"` // 資料名
	IssuerEdinetCode     string `json:"issuerEdinetCode"`
	SubjectEdinetCode    string `json:"subjectEdinetCode"`
	SubsidiaryEdinetCode string `json:"subsidiaryEdinetCode"`
	CurrentReportReason  string `json:"currentReportReason"`
	ParentDocID          string `json:"parentDocID"`
	OpeDateTime          string `json:"opeDateTime"` // Date かも
	WithdrawalStatus     string `json:"withdrawalStatus"`
	DocInfoEditStatus    string `json:"docInfoEditStatus"`
	DisclosureStatus     string `json:"disclosureStatus"`
	XbrlFlag             string `json:"xbrlFlag"`
	PdfFlag              string `json:"pdfFlag"`
	AttachDocFlag        string `json:"attachDocFlag"`
	CsvFlag              string `json:"csvFlag"`
	LegalStatus          string `json:"legalStatus"`
}

type Report struct {
	Metadata Meta     `json:"metadata"`
	Results  []Result `json:"results"`
}

/*
B/S の値のうち比例縮尺図に使うもの
*/
type Summary struct {
	CompanyName               string     `json:"company_name"`
	PeriodStart               string     `json:"period_start"`
	PeriodEnd                 string     `json:"period_end"`
	UnitString                string     `json:"unit_string"`                  // 単位
	CurrentAssets             TitleValue `json:"current_assets"`               // 流動資産
	TangibleAssets            TitleValue `json:"tangible_assets"`              // 有形固定資産
	IntangibleAssets          TitleValue `json:"intangible_assets"`            // 無形固定資産
	InvestmentsAndOtherAssets TitleValue `json:"investments_and_other_assets"` // 投資その他の資産
	CurrentLiabilities        TitleValue `json:"current_liabilities"`          // 流動負債
	FixedLiabilities          TitleValue `json:"fixed_liabilities"`            // 固定負債
	NetAssets                 TitleValue `json:"net_assets"`                   // 純資産
}

type PLSummary struct {
	CompanyName         string     `json:"company_name"`
	PeriodStart         string     `json:"period_start"`
	PeriodEnd           string     `json:"period_end"`
	UnitString          string     `json:"unit_string"`
	CostOfGoodsSold     TitleValue `json:"cost_of_goods_sold"`    // 売上原価
	SGAndA              TitleValue `json:"sg_and_a"`              // 販売費及び一般管理費
	Sales               TitleValue `json:"sales"`                 // 売上高
	OperatingProfit     TitleValue `json:"operating_profit"`      // 営業利益
	OperatingRevenue    TitleValue `json:"operating_revenue"`     // 営業収益
	HasOperatingRevenue bool       `json:"has_operating_revenue"` // 営業収益が計上されているかどうか
	OperatingCost       TitleValue `json:"operating_cost"`        // 営業費用 (一部の企業で使用)
	HasOperatingCost    bool       `json:"has_operating_cost"`    // 営業費用が計上されているかどうか
	// OperatingLoss             TitleValue `json:"operating_loss"` // 営業損失
}

type Fundamental struct {
	CompanyName         string `json:"company_name"`
	PeriodStart         string `json:"period_start"`
	PeriodEnd           string `json:"period_end"`
	Sales               int    `json:"sales"`
	OperatingProfit     int    `json:"operating_profit"`
	OperatingRevenue    int    `json:"operating_revenue"`     // 営業収益
	HasOperatingRevenue bool   `json:"has_operating_revenue"` // 営業収益が計上されているかどうか
	OperatingCost       int    `json:"operating_cost"`        // 営業費用 (一部の企業で使用)
	HasOperatingCost    bool   `json:"has_operating_cost"`    // 営業費用が計上されているかどうか
	Liabilities         int    `json:"liabilities"`
	NetAssets           int    `json:"net_assets"`
}

type CFSummary struct {
	CompanyName string     `json:"company_name"`
	PeriodStart string     `json:"period_start"`
	PeriodEnd   string     `json:"period_end"`
	UnitString  string     `json:"unit_string"`
	OperatingCF TitleValue `json:"operating_cf"` // 営業活動によるキャッシュ・フロー
	InvestingCF TitleValue `json:"investing_cf"` // 投資活動によるキャッシュ・フロー
	FinancingCF TitleValue `json:"financing_cf"` // 財務活動によるキャッシュ・フロー
	// FreeCF TitleValue `json:"free_cf"` // フリーCF
	StartCash TitleValue `json:"start_cash"` // 現金及び現金同等物の期首残高
	EndCash   TitleValue `json:"end_cash"`   // 現金及び現金同等物の期末残高
}

type FailedReport struct {
	DocID        string `json:"doc_id"`
	RegisterDate string `json:"register_date"`
	ErrorMsg     string `json:"error_msg"`
}

type InvalidSummary struct {
	DocID        string `json:"doc_id"`
	RegisterDate string `json:"register_date"`
	ErrorMsg     string `json:"error_msg"`
	CompanyName  string `json:"company_name"`
	SummaryType  string `json:"report_type"`
}

type NewsData struct {
	Title   string `json:"title"`
	Summary string `json:"summary"`
	Link    string `json:"link"`
}

type NewsResult struct {
	NewsList []NewsData `json:"news_list"`
	DateStr  string     `json:"date_str"`
	AmPm     string     `json:"am_pm"` // "am" か "pm" を設定
}
