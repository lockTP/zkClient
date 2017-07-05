package zkclient

import (
	"fmt"
	"time"
	"github.com/samuel/go-zookeeper/zk"
	j "github.com/ricardolonga/jsongo"
	"os"
	"github.com/spf13/viper"
)
/*
	Zkclient是一个帮助项目获取zookeeper节点内容并生成json临时文件的中间件，通常用于生成并更新项目配置文件
	生成的目标文件名称为zkConfig.json
	输入参数为:
	address: zookeeper地址
	scheme、auth：节点授权
	zkrootpath：zookeeper中需要获取的目标节点路径
	filepath：项目放置配置文件的文件路径

	使用详情可参考zkclient_test测试类

	一、加载zk配置方式：
	A、用于普通项目方式（自己新建zkConfig的配置文件）
	1、修改common.SetupConfig()函数，即新增以下代码（输入参数可根据项目需要调整，以下为测试输入）
	//初始化话zkclient对象
	zkerr := zkclient.SetupZk(viper.GetString("zkpath"))
	if zkerr != nil {
		log.Fatalf("Load zookeeper config fail: %v", zkerr.Error())
	}


	2.新增zookeeper配置文件zkConfig.json，此文件根据项目配置，不改动，需要参数如下
	{
	  "address": "XXX.XXX.X.XXXX:XXXX",
	  "scheme": "XXX",
	  "auth": "XXX",
	  "zkrootpath": "XXX/XXX/XX",
	  "filepath": "XXX"
	}

	B、用于项目（自带配置文件，即把zk配置写入配置文件之中，不单独调用配置文件）
	1、修改common.SetupConfig()函数，即新增以下代码（输入参数可根据项目需要调整，以下为测试输入）
	//初始化话zkclient对象
	zkerr := zkclient.SetupAuraZk()
	if zkerr != nil {
		log.Fatalf("Load zookeeper config fail: %v", zkerr.Error())
	}

	2.新增项目配置文件节点，此文件根据项目配置，不改动，需要参数如下
	 "zkConfig":{
		 "address": "XXX.XXX.X.XXXX:XXXX",
		  "scheme": "XXX",
		  "auth": "XXX",
		  "zkrootpath": "XXX/XXX/XX",
		  "filepath": "XXX"
	  }


	三、使用zookeeper节点参数
	zkclient.GetString("xxxx")
	如; zkclient.GetString("version")

 */


var (
	zkviper    *viper.Viper
	ADDRESS    string
	SCHEME     string
	AUTH       string
	ZKROOTPATH string
	FILEPATH   string
)


/*
	初始化zk，获取节点参数，生成系统配置临时文件(通过配置文件生成)
 */
func SetupZk(path string) error {
	zkviper = viper.New()

	zkviper.SetConfigName(path)
	zkviper.AddConfigPath("./")

	err := zkviper.ReadInConfig()
	check(err)

	ADDRESS = zkviper.GetString("address")
	SCHEME = zkviper.GetString("scheme")
	AUTH = zkviper.GetString("auth")
	ZKROOTPATH = zkviper.GetString("zkrootpath")
	FILEPATH = zkviper.GetString("filepath")

	//处理zk流程
	process()


	//生成zk的配置读取viper
	zkviper.SetConfigName("./" + FILEPATH)
	zkviper.AddConfigPath("./")
	verr := zkviper.ReadInConfig()
	if verr != nil {
		return verr
	}
	return nil
}

/*
	初始化zk，获取节点参数，生成系统配置临时文件(通过aura内部配置文件生成)
 */
func SetupAuraZk() error {

	ADDRESS = viper.GetString("zkConfig.address")
	SCHEME = viper.GetString("zkConfig.scheme")
	AUTH = viper.GetString("zkConfig.auth")
	ZKROOTPATH = viper.GetString("zkConfig.zkrootpath")
	FILEPATH = viper.GetString("zkConfig.filepath")

	//处理zk流程
	err := process()
	if err != nil {
		return err
	}

	//生成zk的配置读取viper
	zkviper = viper.New()
	zkviper.SetConfigName("./" + FILEPATH)
	zkviper.AddConfigPath("./")
	verr := zkviper.ReadInConfig()
	if verr != nil {
		return verr
	}
	return nil

}

/**
	zk文件主要核心流程
 */
func process() error{
	c, err := loadconf(ADDRESS, SCHEME, AUTH)   //加载zk配置
	if err != nil {
		return err
	}
	berr := buildUpConfigFile(c)   //将zk节点生成临时json配置文件
	if berr != nil {
		return berr
	}
	aerr := addWatch(ZKROOTPATH, c)   //增加监听，若节点修改则更新配置文件
	if aerr != nil {
		return aerr
	}
	return nil
}




/*
	给每一个节点添加监听
 */
func addWatch(rootPath string, c *zk.Conn) error {
	children, _, err := c.Children(rootPath)
	if err != nil {
		return err
	}
	childrenLen := len(children)
	if (childrenLen > 0) {
		for child := range children {
			addWatch(rootPath + "/" + children[child], c)
		}
	} else {
		go watchingNodeValue(rootPath, c)
	}
	return nil
}

/*
	监听节点值变化，如果节点被删除，则处理异常
 */
func watchingNodeValue(rootPath string, c *zk.Conn) {
	for {
		_, _, ch, err := c.GetW(rootPath)
		check(err)
		e := <-ch
		fmt.Println("Node value has been changed:", e.Type, "Event:", e)
		reloadConfig(c)
	}
}

/*
	重新生成配置文件，且使用viper重新读取配置文件
 */
func reloadConfig(c *zk.Conn) {
	buildUpConfigFile(c)
	error := zkviper.ReadInConfig()
	if error != nil {
		fmt.Println("Read config fail: %v", error.Error())
	}
}


/*
	根据zk初始化生成配置文件
 */
func buildUpConfigFile(c *zk.Conn) error {
	ret := j.Object()
	_, err := nodeToj(c, ZKROOTPATH, "configuration", ret)
	if err != nil {
		return err
	}
	fileerr := jTofile(ret, FILEPATH + ".json")
	if fileerr != nil {
		return fileerr
	}
	return nil
}

/*
	加载zk配置项
 */
func loadconf(address string, scheme string, auth string) (*zk.Conn, error) {
	c, _, err := zk.Connect([]string{address}, time.Second)
	c.AddAuth(scheme, []byte(auth))
	if err != nil {
		return nil, err
	}
	return c, nil
}

/*
	依据节点生成json格式数据
 */
func nodeToj(c *zk.Conn, nodepath string, nodename string, ret j.O) (j.O, error) {
	children, _, err := c.Children(nodepath)
	if err != nil {
		return nil, fmt.Errorf("There is no node in the path. The authentication might be wrong.")
	}
	childrenLen := len(children)
	if (childrenLen > 0) {
		childJ := j.Object()
		for child := range children {
			nodeToj(c, nodepath + "/" + children[child], children[child], childJ)
		}
		ret.Put(nodename, childJ)
	} else {
		content, _, err := c.Get(nodepath)
		if err != nil {
			return nil, err
		}
		ret.Put(nodename, string(content))
	}
	return ret, nil
}

/*
	将json配置数据生成临时配置文件供项目读取
 */
func jTofile(zkConfig j.O, path string) error {
	data, ok := zkConfig.Get("configuration").(j.O)
	if !ok {
		return fmt.Errorf("The content is not the type of json.")
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	d2 := []byte(data.String())
	n2, err := f.Write(d2)
	if err != nil {
		return err
	}
	fmt.Printf("wrote %d bytes in temp.json\n", n2)
	return nil
}

/*
	使用zkclient读取配置节点参数
 */
func GetString(key string) string {
	return zkviper.GetString(key)
}

func GetBool(key string) bool {
	return zkviper.GetBool(key)
}

func GetInt(key string) int {
	return zkviper.GetInt(key)
}

func GetInt64(key string) int64 {
	return zkviper.GetInt64(key)
}

func GetFloat64(key string) float64 {
	return zkviper.GetFloat64(key)
}

func GetTime(key string) time.Time {
	return zkviper.GetTime(key)
}

func GetDuration(key string) time.Duration {
	return zkviper.GetDuration(key)
}

func GetStringSlice(key string) []string {
	return zkviper.GetStringSlice(key)
}

func GetStringMap(key string) map[string]interface{} {
	return zkviper.GetStringMap(key)
}

func GetStringMapString(key string) map[string]string {
	return zkviper.GetStringMapString(key)
}

func GetSizeInBytes(key string) uint {
	return zkviper.GetSizeInBytes(key)
}

/*
	zkclient异常处理
 */
func check(e error) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("zkClient工具包错误：", err)
		}
	}()
	if e != nil {
		panic(e)
	}
}
