package command

import (
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/denisbrodbeck/machineid"
	"os"
	"os/user"
	"path/filepath"
	"strings"
)

var cipherKey util.CipherKey = []byte("OP7XUA5SFHREMB9N0L814ZTJ6QIYDVCW")

func init() {
	cmdToken.Run = runToken // break init cycle
}

var cmdToken = &Command{
	UsageLine: "token -secert=***",
	Short:     "generate mount access certificate",
	Long: `generate mount access certificate".
  `,
}

var env = map[string]string{
	"1": "http://10.17.100.28:9501",
	"2": "http://10.17.100.28:9501",
	"0": "http://10.17.100.28:9501",
}

var (
	secretKey = cmdToken.Flag.String("secret", "", "连接实例的密钥")
	envKey    = cmdToken.Flag.String("env", "0", "选择一个要连接的集群 [0. 测试][1. 大屯][2. 亦庄]")
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
		return true
	}

	content := *secretKey + "," + mid + "," + env[*envKey]
	encryptedData, encryptionErr := util.Encrypt([]byte(content), cipherKey)
	if encryptionErr != nil {
		println(encryptionErr.Error())
		return true
	}

	u, _ := user.Current()
	os.MkdirAll(filepath.Join(u.HomeDir, ".galaxy"), 0755)
	f, err := os.OpenFile(filepath.Join(u.HomeDir, ".galaxy", "mount-auth"), os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		println(err.Error())
		return true
	}
	defer f.Close()
	f.Write(encryptedData)
	return true
}
