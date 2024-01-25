package main

import (
	"fmt"
	"log"
	"os"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/data"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
)

func main() {
	conf, err := odps.NewConfigFromIni(os.Args[1])
	if err != nil {
		log.Fatalf("%+v", err)
	}

	aliAccount := account.NewAliyunAccount(conf.AccessId, conf.AccessKey)
	odpsIns := odps.NewOdps(aliAccount, conf.Endpoint)
	odpsIns.SetDefaultProjectName(conf.ProjectName)
	project := odpsIns.DefaultProject()
	tunnelEndpoint, err := project.GetTunnelEndpoint()
	if err != nil {
		log.Fatalf("%+v", err)
	}
	fmt.Println("tunnel endpoint: " + tunnelEndpoint)
	tunnelIns := tunnel.NewTunnel(odpsIns, tunnelEndpoint)

	// create table `json_table` ddl:
	// CREATE TABLE IF NOT EXISTS project_name.json_table(json_field JSON) STORED AS ALIORC;
	session, err := tunnelIns.CreateUploadSession(
		project.Name(),
		"json_table",
		tunnel.SessionCfg.WithDefaultDeflateCompressor(),
	)

	if err != nil {
		log.Fatalf("%+v", err)
	}
	tableSchema := session.Schema()
	log.Printf("schema: %+v", tableSchema)

	recordWriter, err := session.OpenRecordWriter(0)

	nullValue := data.NewJson(nil)
	record := []data.Data{
		nullValue,
	}
	err = recordWriter.Write(record)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	b := true
	booleanValue := data.NewJson(b)
	record = []data.Data{
		booleanValue,
	}
	err = recordWriter.Write(record)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	sliceValue := data.NewJson([]interface{}{"abc", "dfg"})
	record = []data.Data{
		sliceValue,
	}
	err = recordWriter.Write(record)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	m := struct {
		Age  int
		Name string
	}{
		Age:  20,
		Name: "Ali",
	}
	objectValue := data.NewJson(m)
	record = []data.Data{
		objectValue,
	}
	err = recordWriter.Write(record)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	stringValue := data.NewJson("asdfghjkl")
	record = []data.Data{
		stringValue,
	}
	err = recordWriter.Write(record)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	emptyStringValue := data.NewJson("")
	record = []data.Data{
		emptyStringValue,
	}
	err = recordWriter.Write(record)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	intValue := data.NewJson(123456)
	record = []data.Data{
		intValue,
	}
	err = recordWriter.Write(record)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	doubleValue := data.NewJson(123.456)
	record = []data.Data{
		doubleValue,
	}
	err = recordWriter.Write(record)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	err = recordWriter.Close()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	err = session.Commit([]int{0})
	if err != nil {
		log.Fatalf("%+v", err)
	}
}
