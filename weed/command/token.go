package command

import (
	"fmt"
	"github.com/chrislusf/seaweedfs/weed/util"
	"github.com/denisbrodbeck/machineid"
	"net/url"
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

/**
galaxyfs-prd.ihuman.com   生产环境
galaxyfs-in.dev.ihuman.com 内网环境
galaxyfs-dev.dev.ihuman.com  dev环境
galaxyfs-test.dev.ihuman.com  test环境
*/
var env = map[string]string{
	"0": "https://galaxyfs-dev.dev.ihuman.com",
	"1": "https://galaxyfs-in.dev.ihuman.com",
	"2": "https://galaxyfs-test.dev.ihuman.com",
	"3": "https://galaxyfs-prd.ihuman.com",
}

var (
	name      = cmdToken.Flag.String("name", "", "")
	secretKey = cmdToken.Flag.String("secret", "", "")
	envKey    = cmdToken.Flag.String("env", "0", "")
)

func runToken(cmd *Command, args []string) bool {
	fmt.Print("输入一个要连接的集群的域名\n")
	fmt.Scanln(envKey)
	addr, err := url.Parse(*envKey)
	if err != nil || addr.Scheme == "" {
		fmt.Println("Illegal input")
		return true
	}
	fmt.Print("输入需要连接的实例名称: ")
	fmt.Scanln(name)
	fmt.Print("输入需要连接的实例密钥: ")
	fmt.Scanln(secretKey)
	*secretKey = strings.TrimSpace(*secretKey)

	if *name == "" {
		println("need name")
		return true
	}

	if *secretKey == "" {
		println("need secret")
		return true
	}

	mid, err := machineid.ID()
	if err != nil {
		println(err.Error())
		return true
	}

	content := *secretKey + "," + mid + "," + *envKey
	encryptedData, encryptionErr := util.Encrypt([]byte(content), cipherKey)
	if encryptionErr != nil {
		println(encryptionErr.Error())
		return true
	}

	u, _ := user.Current()
	os.MkdirAll(filepath.Join(u.HomeDir, ".galaxy"), 0755)
	f, err := os.OpenFile(filepath.Join(u.HomeDir, ".galaxy", *name), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		println(err.Error())
		return true
	}
	defer f.Close()
	f.Write(encryptedData)
	fmt.Println("ok")
	return true
}
