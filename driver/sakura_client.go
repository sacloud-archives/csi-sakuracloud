package driver

import (
	"strconv"
	"time"

	"github.com/sacloud/libsacloud/api"
	"github.com/sacloud/libsacloud/sacloud"
)

func toSakuraID(id string) int64 {
	i, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return 0
	}
	return i
}

type nfsAPIClient interface {
	SetEmpty()
	SetNameLike(name string)
	SetOffset(offset int)
	SetLimit(limit int)

	Find() (*api.SearchNFSResponse, error)

	Read(id int64) (*sacloud.NFS, error)
	Create(value *sacloud.NFS) (*sacloud.NFS, error)
	Delete(id int64) (*sacloud.NFS, error)
	Stop(id int64) (bool, error)

	SleepUntilUp(id int64, timeout time.Duration) error
	SleepUntilDown(id int64, timeout time.Duration) error
	SleepWhileCopying(id int64, timeout time.Duration, maxRetry int) error
}
