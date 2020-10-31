package main

import (
	"github.com/xp/shorttext-db/config"
	"github.com/xp/shorttext-db/easymr/collaborator"
	"github.com/xp/shorttext-db/memkv"
	"github.com/xp/shorttext-db/server"
	"github.com/xp/shorttext-db/utils"
)

func main() {
	config.LoadSettings("/opt/test/config/test_case2.txt", nil)
	collaborator.GetCollaborator()
	memkv.NewDBServer(server.GetNode())
	utils.WaitFor()
}
