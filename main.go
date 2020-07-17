package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
)

const (
	cmapiurl               = "http://172.16.102.95:8090/api/customerManagement/v1/customers"
	cmapitoken             = "Basic Y21hcGlfYWdlbnQ6cHZeOSZUL3BVakhhRStuXQ=="
	oauthurl               = "http://172.16.102.95:9099/users"
	oauthtoken             = "Bearer ZTEYOGU0YTCTZJUWMS0ZNTJKLTG4NZMTNZDIM2FMOGQ4ZGRH"
	getustomerscount       = 10
	filecsvcustomers       = "customers.csv"
	filecsvcreatecustomers = "create_customers.csv"
	gorutines              = 5
)

type CSVCreateCustomer struct {
	ID              string
	Login           string
	Password        string
	SoauthID        string
	SoauthParentID  string
	ResponseCode    string
	ResponseMessage string
}
type Config struct {
	CMAPIUrl       string
	CMAPIToken     string
	OAUTHToken     string
	OAUTHUrl       string
	CustomersCount int
	Range          Range
}

type Range struct {
	From int
	To   int
}

type SOAUTHResponse struct {
	ID       string
	Login    string
	ParentID string
}

type Customer struct {
	ID       int    `json:"id,omitempty"`
	Login    string `json:"login,omitempty"`
	Password string `json:"password,omitempty"`
}

type SOAUTHRequest struct {
	Login    string `json:"login"`
	Password string `json:"password"`
}

func getConfig() Config {
	return Config{
		CMAPIUrl:       cmapiurl,
		CMAPIToken:     cmapitoken,
		CustomersCount: getustomerscount,
		OAUTHUrl:       oauthurl,
		OAUTHToken:     oauthtoken,
	}
}

func getCustomers(cfg Config) (customers []Customer, maxCount int, err error) {
	req := &http.Request{
		Method: http.MethodGet,
		Header: http.Header{
			"Authorization": {cfg.CMAPIToken},
			"Range":         {"customers=" + strconv.Itoa(cfg.Range.From) + "-" + strconv.Itoa(cfg.Range.To)},
		},
	}

	req.URL, _ = url.Parse(cfg.CMAPIUrl)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, err
	}

	contentRange := resp.Header.Get("Content-Range")

	words := strings.Split(contentRange, "/")

	custCount, _ := strconv.Atoi(words[1])

	defer resp.Body.Close()

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, 0, err
	}

	var customersList []Customer

	_ = json.Unmarshal(respBody, &customersList)

	return customersList, custCount, nil
}

func createCustomer(cfg Config, customer Customer) (*http.Response, error) {
	var soauthReq SOAUTHRequest

	soauthReq.Login = customer.Login
	soauthReq.Password = customer.Password

	marshal, err := json.Marshal(soauthReq)
	if err != nil {
		return nil, err
	}

	reqBody := bytes.NewBuffer(marshal)

	req, err := http.NewRequest(http.MethodPost, cfg.OAUTHUrl, reqBody)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Add("Authorization", cfg.OAUTHToken)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func writeCSVCustomers(customers []Customer) error {
	csvfile, err := os.OpenFile(filecsvcustomers, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0660)

	if err != nil {
		return err
	}

	defer csvfile.Close()

	w := csv.NewWriter(csvfile)

	w.Comma = ','

	for _, customer := range customers {
		custID := strconv.Itoa(customer.ID)

		row := []string{
			custID,
			customer.Login,
			customer.Password,
		}
		err = w.Write(row)
		if err != nil {
			return err
		}

	}

	w.Flush()

	return nil
}

func writeCSVCreateCustomers(customers []CSVCreateCustomer) error {
	csvfile, err := os.OpenFile(filecsvcreatecustomers, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0660)

	if err != nil {
		return err
	}

	defer csvfile.Close()

	w := csv.NewWriter(csvfile)

	w.Comma = ','

	for _, customer := range customers {
		row := []string{
			customer.ID,
			customer.Login,
			customer.Password,
			customer.SoauthID,
			customer.SoauthParentID,
			customer.ResponseCode,
			customer.ResponseMessage,
		}

		err = w.Write(row)
		if err != nil {
			return err
		}

	}

	w.Flush()

	return nil
}

func main() {
	cfg := getConfig()

	cfg.Range = Range{
		From: 11000,
		To:   11000 + getustomerscount,
	}

	customers, maxCustomers, err := getCustomers(cfg)
	if err != nil {
		fmt.Println("get customers err: ", err.Error())
	}

	err = writeCSVCustomers(customers)
	if err != nil {
		fmt.Println("error save csv:", err.Error())
	}

	cfg.Range.From += 1

	//wg := new(sync.WaitGroup)
	//wg.Add(gorutines)

	for count := 0; count <= maxCustomers; {
		var csvCustomersList []CSVCreateCustomer

		for i := 0; i < len(customers); i++ {
			count++

			if customers[i].Login != "" && customers[i].Password != "" {
				resp, err := createCustomer(cfg, customers[i])
				if err != nil {
					fmt.Println("create customer error: ", err.Error())
				} else {
					var csvCustomer CSVCreateCustomer

					if resp.StatusCode == http.StatusCreated {
						respBody, _ := ioutil.ReadAll(resp.Body)

						var soauthResponse SOAUTHResponse

						_ = json.Unmarshal(respBody, &soauthResponse)

						customerID := strconv.Itoa(customers[i].ID)

						csvCustomer.ID = customerID
						csvCustomer.Login = customers[i].Login
						csvCustomer.Password = customers[i].Password
						csvCustomer.SoauthID = soauthResponse.ID
						csvCustomer.SoauthParentID = soauthResponse.ParentID
						csvCustomer.ResponseCode = "201"
						csvCustomer.ResponseMessage = ""
					} else {
						var body string

						if bytes, err := ioutil.ReadAll(resp.Body); err == nil {
							body = string(bytes)
						}

						custID := strconv.Itoa(customers[i].ID)

						csvCustomer.ID = custID
						csvCustomer.Login = customers[i].Login
						csvCustomer.Password = customers[i].Password
						csvCustomer.SoauthID = ""
						csvCustomer.SoauthParentID = ""
						csvCustomer.ResponseCode = strconv.Itoa(resp.StatusCode)
						csvCustomer.ResponseMessage = body
					}

					csvCustomersList = append(csvCustomersList, csvCustomer)
				}
			}
			//wg.Done()

			//wg.Wait()
		}

		err := writeCSVCreateCustomers(csvCustomersList)
		if err != nil {
			fmt.Println("err save csv: ", err.Error())
		}

		cfg.Range.From += getustomerscount
		cfg.Range.To += getustomerscount

		customers, _, err = getCustomers(cfg)
		if err != nil {
			fmt.Println("get customers err: ", err.Error())
		}

		err = writeCSVCustomers(customers)
		if err != nil {
			fmt.Println("error save csv:", err.Error())
		}
	}
}
