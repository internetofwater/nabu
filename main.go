package main

import (
	"nabu/internal/common"
	"nabu/pkg/cli"
)

func main() {
	common.InitLogging()
	cli.Execute()
}
