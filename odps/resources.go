package odps

import (
	"io"
	"net/http"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/common"
	"github.com/pkg/errors"
)

type Resources struct {
	projectName string
	odpsIns     *Odps
}

type ResourceType string

const (
	ResourceTypePy      ResourceType = "py"      // python 文件，文件后缀为.py。用于作为python 语言的UDF
	ResourceTypeJar     ResourceType = "jar"     // jar 文件，文件后缀为 jar。用于作为java语言的UDF
	ResourceTypeFile    ResourceType = "file"    // 后缀无限制
	ResourceTypeArchive ResourceType = "archive" // 压缩文件，文件后缀为.jar/.zip/.tgr.gz/.tar。资源在使用时先自动解压再使用
)

// NewTables if projectName is not set，the default projectName of odps will be used
func NewResources(odpsIns *Odps, projectName ...string) Resources {
	var _projectName string

	if projectName == nil {
		_projectName = odpsIns.DefaultProjectName()
	} else {
		_projectName = projectName[0]
	}

	return Resources{
		projectName: _projectName,
		odpsIns:     odpsIns,
	}
}

func (rs *Resources) CreateResource(name string, resourceType ResourceType, comment string, reader io.Reader) error {
	client := rs.odpsIns.restClient

	rb := common.ResourceBuilder{ProjectName: rs.projectName}
	req, err := client.NewRequest(common.HttpMethod.PostMethod, rb.Resources(), reader)
	if err != nil {
		return err
	}

	req.Header.Set(common.HttpHeaderOdpsResourceName, name)
	req.Header.Set(common.HttpHeaderOdpsResourceType, string(resourceType))
	req.Header.Set(common.HttpHeaderOdpsComment, comment)

	return errors.WithStack(client.DoWithParseFunc(req, func(res *http.Response) error {
		if res.StatusCode != http.StatusCreated {
			return errors.WithStack(errors.New(res.Status))
		}

		return nil
	}))
}

func (rs *Resources) UpdateResource(name string, resourceType ResourceType, comment string, reader io.Reader) error {
	client := rs.odpsIns.restClient

	rb := common.ResourceBuilder{ProjectName: rs.projectName}
	req, err := client.NewRequest(common.HttpMethod.PutMethod, rb.Resource(name), reader)
	if err != nil {
		return err
	}

	req.Header.Set(common.HttpHeaderOdpsResourceName, name)
	req.Header.Set(common.HttpHeaderOdpsResourceType, string(resourceType))
	req.Header.Set(common.HttpHeaderOdpsComment, comment)

	return errors.WithStack(client.DoWithParseFunc(req, func(res *http.Response) error {
		if res.StatusCode != http.StatusOK {
			return errors.WithStack(errors.New(res.Status))
		}

		return nil
	}))
}

func (rs *Resources) DropResource(name string) error {
	client := rs.odpsIns.restClient

	rb := common.ResourceBuilder{ProjectName: rs.projectName}
	req, err := client.NewRequest(common.HttpMethod.DeleteMethod, rb.Resource(name), nil)
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
