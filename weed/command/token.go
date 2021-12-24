package command

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/denisbrodbeck/machineid"
	"os"
	"path/filepath"
	"strings"
)

var cipherKey util.CipherKey = []byte("OP7XUA5SFHREMB9N0L814ZTJ6QIYDVCW")

func init() {
	cmdToken.Run = runToken // break init cycle
}

var cmdToken = &Command{
	UsageLine: "token -secert=*****************",
	Short:     "generate mount access certificate",
	Long: `generate mount access certificate".
  `,
}

var (
	outPath   = cmdToken.Flag.String("output", "", "if not empty, save mount_cert file to this directory")
	secretKey = cmdToken.Flag.String("secret", "", "use [mount] needs to  galaxy-fs instance secret key")
)

func runToken(cmd *Command, args []string) bool {
	*secretKey = strings.TrimSpace(*secretKey)
	if *secretKey == "" {
		println("need secret")
		return false
	}

	mid, err := machineid.ID()
	if err != nil {
		println(err.Error())
		return false
	}

	content := *secretKey + mid
	encryptedData, encryptionErr := util.Encrypt([]byte(content), cipherKey)
	if encryptionErr != nil {
		println(encryptionErr.Error())
		return false
	}
	if *outPath != "" {
		os.WriteFile(filepath.Join(*outPath, "mount_cert"), encryptedData, 0644)
	} else {
		fmt.Println(encryptedData)
	}
	return true
}
