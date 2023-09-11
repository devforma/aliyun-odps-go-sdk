// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package odps

import (
	"net/url"
	"strconv"
	"strings"
	"time"

	account2 "github.com/aliyun/aliyun-odps-go-sdk/odps/account"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/restclient"
	"github.com/pkg/errors"
	"gopkg.in/ini.v1"
)

// Config is the basic config for odps. The NewConfig function should be used, which sets default values.
type Config struct {
	AccessId             string
	AccessKey            string
	StsToken             string
	Endpoint             string
	ProjectName          string
	TcpConnectionTimeout time.Duration
	HttpTimeout          time.Duration
	TunnelEndpoint       string
	TunnelQuotaName      string
	Hints                map[string]string
}

func NewConfig() *Config {
	return &Config{
		TcpConnectionTimeout: 30 * time.Second,
		HttpTimeout:          0,
	}
}

func NewConfigFromIni(iniPath string) (*Config, error) {
	cfg, err := ini.LoadSources(ini.LoadOptions{IgnoreInlineComment: true}, iniPath)

	if err != nil {
		return nil, errors.WithStack(err)
	}

	section := cfg.Section("odps")
	conf := NewConfig()

	conf.AccessId = section.Key("access_id").String()
	conf.AccessKey = section.Key("access_key").String()
	conf.StsToken = section.Key("sts_token").String()
	conf.Endpoint = section.Key("endpoint").String()
	conf.TunnelQuotaName = section.Key("tunnel_quota_name").String()

	_, err = url.Parse(conf.Endpoint)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid endpoint: \"%s\"", conf.Endpoint)
	}

	conf.ProjectName = section.Key("project").String()

	connTimeout, err := section.GetKey("tcp_connection_timeout")
	if err == nil {
		v, err := connTimeout.Int()
		if err == nil {
			conf.TcpConnectionTimeout = time.Duration(v) * time.Second
		}
	}

	httpTimeout, err := section.GetKey("http_timeout")
	if err == nil {
		v, err := httpTimeout.Int()
		if err == nil {
			conf.HttpTimeout = time.Duration(v) * time.Second
		}
	}

	tunnelEndpoint, err := section.GetKey("tunnel_endpoint")
	if err == nil {
		conf.TunnelEndpoint = tunnelEndpoint.String()
	}

	hints := make(map[string]string)
	keys := section.Keys()
	for _, key := range keys {
		if strings.HasPrefix(key.Name(), "hints") {
			splits := strings.SplitN(key.Name(), ".", 2)
			hint := splits[1]
			hints[hint] = key.Value()
		}
	}
	if len(hints) > 0 {
		conf.Hints = hints
	}

	return conf, nil
}

func (c *Config) GenAccount() account2.Account {
	var account account2.Account

	if c.StsToken == "" {
		account = account2.NewAliyunAccount(c.AccessId, c.AccessKey)
	} else {
		account = account2.NewStsAccount(c.AccessId, c.AccessKey, c.StsToken)
	}

	return account
}

func (c *Config) GenRestClient() restclient.RestClient {
	account := c.GenAccount()
	client := restclient.NewOdpsRestClient(account, c.Endpoint)
	client.TcpConnectionTimeout = c.TcpConnectionTimeout
	client.HttpTimeout = c.HttpTimeout

	return client
}

func (c *Config) GenOdps() *Odps {
	account := c.GenAccount()
	odpsIns := NewOdps(account, c.Endpoint)
	odpsIns.SetTcpConnectTimeout(c.TcpConnectionTimeout)
	odpsIns.SetHttpTimeout(c.HttpTimeout)
	odpsIns.SetDefaultProjectName(c.ProjectName)

	return odpsIns
}

func (c *Config) FormatDsn() string {
	u, _ := url.Parse(c.Endpoint)

	dsn := url.URL{
		Scheme: u.Scheme,
		Host:   u.Host,
		Path:   u.Path,
	}
	values := make(url.Values)
	values.Set("project", c.ProjectName)

	if c.StsToken != "" {
		values.Set("stsToken", c.StsToken)
	}

	if c.HttpTimeout > 0 {
		httpTimeout := int64(c.HttpTimeout) / int64(time.Second)
		values.Set("httpTimeout", strconv.FormatInt(httpTimeout, 10))
	}

	if c.TcpConnectionTimeout > 0 {
		connTimeOut := int64(c.TcpConnectionTimeout) / int64(time.Second)
		values.Set("tcpConnectionTimeout", strconv.FormatInt(connTimeOut, 10))
	}

	if c.TunnelQuotaName != "" {
		values.Set("tunnelQuotaName", c.TunnelQuotaName)
	}

	if c.TunnelEndpoint != "" {
		values.Set("tunnelEndpoint", c.TunnelEndpoint)
	}

	if c.Hints != nil {
		for k, v := range c.Hints {
			values.Set(k, v)
		}
	}

	dsn.RawQuery = values.Encode()
	dsn.User = url.UserPassword(c.AccessId, c.AccessKey)

	return dsn.String()
}
