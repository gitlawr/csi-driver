package main

import (
	"github.com/sirupsen/logrus"
	_ "net/http/pprof"
	"os"

	"github.com/urfave/cli"
	"github.com/harvester/csi-driver/pkg/csi"
)

const (
	driverName = "driver.harvesterhci.io"
)

func main() {
	flags := []cli.Flag{
		cli.StringFlag{
			Name:  "endpoint",
			Value: "unix:///csi/csi.sock",
			Usage: "CSI endpoint",
		},
		cli.StringFlag{
			Name:  "drivername",
			Value: driverName,
			Usage: "Name of the CSI driver",
		},
		cli.StringFlag{
			Name:  "nodeid",
			Usage: "Node id",
		},
		cli.StringFlag{
			Name:  "namespace",
			Usage: "Namespace of the resources in the host cluster",
		},
		cli.StringFlag{
			Name:  "kubeconfig",
			Value: "",
			Usage: "kubeconfig to access Harvester",
		},
		cli.StringFlag{
			Name:  "csi-version",
			Value: "0.3.0",
			Usage: "CSI plugin version",
		},
	}

	app := cli.NewApp()

	app.Name = "harvester csi driver"
	app.Version = "dev"
	app.Flags = flags
	app.Action = func(c *cli.Context) {
		if err := runCSI(c); err != nil {
			logrus.Fatalf("Error running CSI driver: %v", err)
		}
	}

	if err := app.Run(os.Args); err != nil {
		logrus.Fatalf("Error running CSI driver: %v", err)
	}
}

func runCSI(c *cli.Context) error {
	manager := csi.GetCSIManager()
	identityVersion := c.App.Version
	logrus.Infoln("drivername: ", c.String("drivername"))
	logrus.Infoln("nodeid: ", c.String("nodeid"))
	logrus.Infoln("endpoint: ", c.String("endpoint"))
	logrus.Infoln("kubeconfig: ", c.String("kubeconfig"))
	return manager.Run(c.String("drivername"),
		c.String("nodeid"),
		c.String("endpoint"),
		c.String("namespace"),
		identityVersion,
		c.String("kubeconfig"))
}
