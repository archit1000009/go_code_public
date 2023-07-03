package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"sync"
	"time"

	//"net/http"
	"database/sql"
	"os"

	//"github.com/gorilla/mux"
	//"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func main() {
	db := db1{}

	db.make_connection()

}

type db1 struct {
	DB sql.DB
}

func (d *db1) make_connection() {
	connStr := "user=readonly_user_archit password=qjqU87lTZA6oA52Ea1rGIKRUnbLxWh dbname=paymeindia host=usdt-instance-1.ckgngqzzn19f.ap-south-1.rds.amazonaws.com port=5987 sslmode=require"

	// Connection to PostgreSQL database
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Pinging the database to check connection
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connection established successfully")

	// Create a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Create channels to receive data from each table
	CustomUserChan := make(chan CustomUser)
	DeviceInfoChan := make(chan DeviceInfo)
	UserTypeChan := make(chan UserType)
	UserBankChan := make(chan UserBank)
	UserDocsChan := make(chan UserDocs)
	processedRecordsChan := make(chan ProcessedRecords)
	done := make(chan struct{})
	//done1 := make(chan struct{})
	wg.Add(1)
	go fetchCustomUserData(db, CustomUserChan, &wg)

	wg.Add(1)
	go fetchDeviceInfoData(db, DeviceInfoChan, &wg)

	wg.Add(1)
	go fetchUserTypeData(db, UserTypeChan, &wg)

	wg.Add(1)
	go fetchUserBankData(db, UserBankChan, &wg)

	wg.Add(1)
	go fetchUserDocsData(db, UserDocsChan, &wg)

	// Start a goroutine to process the fetched loan data entries and generate the report
	wg.Add(1)
	go ProcessRecords(CustomUserChan, DeviceInfoChan, UserTypeChan, UserBankChan, UserDocsChan, processedRecordsChan, &wg)

	//wg.Add(1)
	//go writeDataToCSV("loan_data.csv", "borrower_data.csv", "interest_rate.csv", CustomUserChan, DeviceInfoChan, UserTypeChan, UserBankChan, UserDocsChan, done1, &wg)
	// Wait for all goroutines to finish
	wg.Wait()

	//csv file
	// Opening the CSV file for writing
	file, err := os.Create("compiled_data.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Creating a CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write the processed loan data to the CSV file
	for processedData := range processedRecordsChan {
		record := []string{
			fmt.Sprintf("%d", processedData.max_credit_limit),
			processedData.first_name,
			processedData.last_name,
			processedData.address,
		}

		writer.Write(record)
	}

	close(done)
}

type ProcessedRecords struct {
	address                   string
	auto                      bool
	blocked                   bool
	blocked_date_time         time.Time
	blocked_reason            string
	can_apply_date_time       time.Time
	can_apply_for_loan        bool
	date_of_birth             time.Time
	fathers_name              string
	first_name                string
	gender                    string
	interested                bool
	last_name                 string
	not_interested_reason     string
	phone_number              string
	phone_number_verified     bool
	max_credit_limit          int
	device_info               string
	name                      string
	account_number            string
	bank_name                 string
	bene_name_from_bank       *string
	ifsc_code                 string
	verified                  string
	address_proof_verified    string
	adhar_card_verified       string
	bank_statement_password   *string
	bank_statement_verified   string
	cibil_score               int
	cibil_score_verified      string
	digital_ekyc              *bool
	office_email_id           *string
	office_email_id_verified  string
	office_extension          *string
	office_extension_verified string
	office_id_verified        string
	pan_card_verified         string
	reference_1_name          *string
	reference_1_phone         *string
	reference_2_name          *string
	reference_2_phone         *string
	references_verified       string
	salary_slip_verified      string
	social_score_verified     string
}

type CustomUser struct {
	user_id               int
	phone_number          string
	first_name            string
	last_name             string
	can_apply_for_loan    bool
	blocked               bool
	address               string
	date_of_birth         time.Time
	gender                string
	interested            bool
	fathers_name          string
	blocked_reason        string
	not_interested_reason string
	max_credit_limit      int
	blocked_date_time     time.Time
	can_apply_date_time   time.Time
	auto                  bool
	phone_number_verified bool
}

type DeviceInfo struct {
	id          int
	device_info string
	user_id     int
}

type UserType struct {
	id   int
	name string
}

type UserBank struct {
	user_id             int
	bank_name           string
	account_number      string
	ifsc_code           string
	verified            string
	bene_name_from_bank *string
}

type UserDocs struct {
	user_id                   int
	address_proof_verified    string
	pan_card_verified         string
	adhar_card_verified       string
	office_id_verified        string
	salary_slip_verified      string
	office_email_id           *string
	office_email_id_verified  string
	office_extension          *string
	office_extension_verified string
	reference_1_name          *string
	reference_1_phone         *string
	reference_2_name          *string
	reference_2_phone         *string
	references_verified       string
	social_score_verified     string
	bank_statement_verified   string
	bank_statement_password   *string
	cibil_score               int
	cibil_score_verified      string
	digital_ekyc              *bool
}

// Fetch custom user data from the database and send it to the CustomUserChan channel
func fetchCustomUserData(db *sql.DB, CustomUserChan chan<- CustomUser, wg *sync.WaitGroup) {
	defer wg.Done()

	rows, err := db.Query(`SELECT user_id,phone_number,first_name,last_name,can_apply_for_loan,blocked,address,date_of_birth,
	gender,interested,fathers_name,blocked_reason,not_interested_reason,max_credit_limit,blocked_date_time,
	can_apply_date_time,auto,phone_number_verified FROM user_manage_usertype`)
	if err != nil {
		log.Println(err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var customuser CustomUser
		err := rows.Scan(&customuser.user_id, &customuser.phone_number, &customuser.first_name,
			&customuser.last_name, &customuser.can_apply_for_loan, &customuser.blocked, &customuser.address,
			&customuser.date_of_birth, &customuser.gender, &customuser.interested, &customuser.fathers_name,
			&customuser.blocked_reason, &customuser.not_interested_reason, &customuser.max_credit_limit,
			&customuser.blocked_date_time, &customuser.can_apply_date_time, &customuser.auto,
			&customuser.phone_number_verified)
		if err != nil {
			log.Println(err)
			continue
		}

		CustomUserChan <- customuser
	}

	close(CustomUserChan)
}

// Fetch device info data from the database and send it to the DeviceInfoChan channel
func fetchDeviceInfoData(db *sql.DB, DeviceInfoChan chan<- DeviceInfo, wg *sync.WaitGroup) {
	defer wg.Done()

	rows, err := db.Query("SELECT id,device_info,user_id FROM user_manage_deviceinfomodel")
	if err != nil {
		log.Println(err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var deviceinfo DeviceInfo
		err := rows.Scan(&deviceinfo.id, &deviceinfo.device_info, &deviceinfo.user_id)
		if err != nil {
			log.Println(err)
			continue
		}

		DeviceInfoChan <- deviceinfo
	}

	close(DeviceInfoChan)
}

// Fetch user type data from the database and send it to the UserTypeChan channel
func fetchUserTypeData(db *sql.DB, UserTypeChan chan<- UserType, wg *sync.WaitGroup) {
	defer wg.Done()

	rows, err := db.Query("SELECT * FROM user_manage_usertype")
	if err != nil {
		log.Println(err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var usertype UserType
		err := rows.Scan(&usertype.id, &usertype.name)
		if err != nil {
			log.Println(err)
			continue
		}

		UserTypeChan <- usertype
	}

	close(UserTypeChan)
}

// Fetch user bank data from the database and send it to the UserBankChan channel
func fetchUserBankData(db *sql.DB, UserBankChan chan<- UserBank, wg *sync.WaitGroup) {
	defer wg.Done()

	rows, err := db.Query(`SELECT user_id,bank_name,account_number,ifsc_code,verified,bene_name_from_bank
	FROM user_manage_userbankdetailsmodel`)
	if err != nil {
		log.Println(err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var userbank UserBank
		err := rows.Scan(&userbank.user_id, &userbank.bank_name, &userbank.account_number,
			&userbank.ifsc_code, &userbank.verified, &userbank.bene_name_from_bank)
		if err != nil {
			log.Println(err)
			continue
		}

		UserBankChan <- userbank
	}

	close(UserBankChan)
}

// Fetch user bank data from the database and send it to the UserBankChan channel
func fetchUserDocsData(db *sql.DB, UserDocsChan chan<- UserDocs, wg *sync.WaitGroup) {
	defer wg.Done()

	rows, err := db.Query(`SELECT user_id,address_proof_verified,pan_card_verified,adhar_card_verified
	,office_id_verified,salary_slip_verified,office_email_id,office_email_id_verified,office_extension,
	office_extension_verified,reference_1_name,reference_1_phone,reference_2_name,reference_2_phone,
	references_verified,social_score_verified,bank_statement_verified,bank_statement_password,
	cibil_score,cibil_score_verified,digital_ekyc FROM user_manage_userdocumentsmodel`)
	if err != nil {
		log.Println(err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var userdocs UserDocs
		err := rows.Scan(&userdocs.user_id, &userdocs.address_proof_verified, &userdocs.pan_card_verified,
			&userdocs.adhar_card_verified, &userdocs.office_id_verified, &userdocs.salary_slip_verified,
			&userdocs.office_email_id, &userdocs.office_email_id_verified, &userdocs.office_extension,
			&userdocs.office_extension_verified, &userdocs.reference_1_name, &userdocs.reference_1_phone,
			&userdocs.reference_2_name, &userdocs.reference_2_phone, &userdocs.references_verified,
			&userdocs.social_score_verified, &userdocs.bank_statement_verified, &userdocs.bank_statement_password,
			&userdocs.cibil_score, &userdocs.cibil_score_verified, &userdocs.digital_ekyc)
		if err != nil {
			log.Println(err)
			continue
		}

		UserDocsChan <- userdocs
	}

	close(UserDocsChan)
}

func ProcessRecords(CustomUserChan <-chan CustomUser, DeviceInfoChan <-chan DeviceInfo, UserTypeChan <-chan UserType, UserBankChan <-chan UserBank, UserDocsChan <-chan UserDocs, processedRecordsChan chan<- ProcessedRecords, wg *sync.WaitGroup) {
	defer wg.Done()

	customUserMap := make(map[int]CustomUser)
	userTypeMap := make(map[int]UserType)
	userBankMap := make(map[int]UserBank)
	userDocsMap := make(map[int]UserDocs)

	for customUserData := range CustomUserChan {
		customUserMap[customUserData.user_id] = customUserData
	}

	for userTypeData := range UserTypeChan {
		userTypeMap[userTypeData.id] = userTypeData
	}

	for userBankData := range UserBankChan {
		userBankMap[userBankData.user_id] = userBankData
	}

	for userDocsData := range UserDocsChan {
		userDocsMap[userDocsData.user_id] = userDocsData
	}

	for deviceInfo := range DeviceInfoChan {
		customUserData, customUserDataExists := customUserMap[deviceInfo.user_id]
		userTypeData, userTypeDataExists := userTypeMap[deviceInfo.id]
		userBankData, userBankDataExists := userBankMap[deviceInfo.user_id]
		userDocsData, userDocsDataExists := userDocsMap[deviceInfo.user_id]

		if !customUserDataExists || !userTypeDataExists || !userBankDataExists || !userDocsDataExists {
			continue
		}

		processedRecords := ProcessedRecords{
			address:                   customUserData.address,
			auto:                      customUserData.auto,
			blocked:                   customUserData.blocked,
			blocked_date_time:         customUserData.blocked_date_time,
			blocked_reason:            customUserData.blocked_reason,
			can_apply_date_time:       customUserData.can_apply_date_time,
			can_apply_for_loan:        customUserData.can_apply_for_loan,
			date_of_birth:             customUserData.date_of_birth,
			fathers_name:              customUserData.fathers_name,
			first_name:                customUserData.first_name,
			gender:                    customUserData.gender,
			interested:                customUserData.interested,
			last_name:                 customUserData.last_name,
			not_interested_reason:     customUserData.not_interested_reason,
			phone_number:              customUserData.phone_number,
			phone_number_verified:     customUserData.phone_number_verified,
			max_credit_limit:          customUserData.max_credit_limit,
			device_info:               deviceInfo.device_info,
			name:                      userTypeData.name,
			account_number:            userBankData.account_number,
			bank_name:                 userBankData.bank_name,
			bene_name_from_bank:       userBankData.bene_name_from_bank,
			ifsc_code:                 userBankData.ifsc_code,
			verified:                  userBankData.verified,
			address_proof_verified:    userDocsData.address_proof_verified,
			adhar_card_verified:       userDocsData.adhar_card_verified,
			bank_statement_password:   userDocsData.bank_statement_password,
			bank_statement_verified:   userDocsData.bank_statement_verified,
			cibil_score:               userDocsData.cibil_score,
			cibil_score_verified:      userDocsData.cibil_score_verified,
			digital_ekyc:              userDocsData.digital_ekyc,
			office_email_id:           userDocsData.office_email_id,
			office_email_id_verified:  userDocsData.office_email_id_verified,
			office_extension:          userDocsData.office_extension,
			office_extension_verified: userDocsData.office_extension_verified,
			office_id_verified:        userDocsData.office_id_verified,
			pan_card_verified:         userDocsData.pan_card_verified,
			reference_1_name:          userDocsData.reference_1_name,
			reference_1_phone:         userDocsData.reference_1_phone,
			reference_2_name:          userDocsData.reference_2_name,
			reference_2_phone:         userDocsData.reference_2_phone,
			references_verified:       userDocsData.references_verified,
			salary_slip_verified:      userDocsData.salary_slip_verified,
			social_score_verified:     userDocsData.social_score_verified,
		}

		processedRecordsChan <- processedRecords
	}

	close(processedRecordsChan)

}
