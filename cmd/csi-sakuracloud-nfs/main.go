package main

import (
	"flag"
	"log"

	"github.com/sacloud/csi-sakuracloud/driver"
)

var (
	defaultEndpoint = "unix:///var/lib/kubelet/plugins/jp.ne.sakura.csi.nfs/csi.sock"
	defaultZone     = "is1b"
)

func main() {
	var (
		endpoint = flag.String("endpoint", defaultEndpoint, "CSI endpoint")
		token    = flag.String("token", "", "SakuraCloud access token")
		secret   = flag.String("secret", "", "SakuraCloud access token secret")
		zone     = flag.String("zone", defaultZone, "SakuraCloud default zone[is1a/is1b/tk1a/tk1v]")
	)
	flag.Parse()

	// TODO validate required
	// TODO enable options from env

	drv, err := driver.NewDriver(*endpoint, *token, *secret, *zone)
	if err != nil {
		log.Fatalln(err)
	}

	if err := drv.Run(); err != nil {
		log.Fatalln(err)
	}
}
