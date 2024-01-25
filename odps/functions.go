package odps

import (
	"encoding/xml"
	"net/http"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/pkg/errors"
)

type Functions struct {
	projectName string
	odpsIns     *Odps
}

// NewTables if projectName is not set，the default projectName of odps will be used
func NewFunctions(odpsIns *Odps, projectName ...string) Functions {
	var _projectName string

	if projectName == nil {
		_projectName = odpsIns.DefaultProjectName()
	} else {
		_projectName = projectName[0]
	}

	return Functions{
		projectName: _projectName,
		odpsIns:     odpsIns,
	}
}

type FunctionResourceModel struct {
	ResourceName []string `xml:"ResourceName"`
}
type FunctionModel struct {
	XMLName   xml.Name              `xml:"Function"`
	Alias     string                `xml:"Alias"`
	ClassType string                `xml:"ClassType"`
	Resources FunctionResourceModel `xml:"Resources"`
}

func (fns *Functions) CreateFunction(name string, classType string, resources []string) error {
	client := fns.odpsIns.restClient

	rb := common.ResourceBuilder{ProjectName: fns.projectName}

	f := FunctionModel{
		Alias:     name,
		ClassType: classType,
		Resources: FunctionResourceModel{ResourceName: resources},
	}

	return errors.WithStack(client.DoXmlWithParseFunc(common.HttpMethod.PostMethod, rb.Functions(), nil, nil, &f, func(res *http.Response) error {
		if res.StatusCode != http.StatusCreated {
			return errors.WithStack(errors.New(res.Status))
		}

		return nil
	}))
}

func (fns *Functions) UpdateFunction(name string, classType string, resources []string) error {
	client := fns.odpsIns.restClient

	rb := common.ResourceBuilder{ProjectName: fns.projectName}

	f := FunctionModel{
		Alias:     name,
		ClassType: classType,
		Resources: FunctionResourceModel{ResourceName: resources},
	}

	return errors.WithStack(client.DoXmlWithParseFunc(common.HttpMethod.PutMethod, rb.Function(name), nil, nil, &f, func(res *http.Response) error {
		if res.StatusCode != http.StatusOK {
			return errors.WithStack(errors.New(res.Status))
		}

		return nil
	}))
}

func (fns *Functions) DropFunction(name string) error {
	client := fns.odpsIns.restClient

	rb := common.ResourceBuilder{ProjectName: fns.projectName}
	req, err := client.NewRequest(common.HttpMethod.DeleteMethod, rb.Function(name), nil)
	if err != nil {
		return err
	}

	return errors.WithStack(client.DoWithParseFunc(req, func(res *http.Response) error {
		if res.StatusCode != http.StatusNoContent {
			return errors.WithStack(errors.New(res.Status))
		}

		return nil
	}))
}
