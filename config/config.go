// config/config.go
package config

import (
	"errors"
	"sync"
	"time"
)

// LocalConfig 本地配置（从 config.yaml 读取）
type LocalConfig struct {
	Base            *BaseConfig    `mapstructure:"base"`
	FeiZhuSdkConfig *SDKGameConfig `mapstructure:"feizhu-game"`
	WeiXinSdkConfig *SDKGameConfig `mapstructure:"weixin-game"`
	DouyinSdkConfig *SDKGameConfig `mapstructure:"douyin-game"`
}

// XxlConfig XXL配置中心配置
type XxlConfig struct {
	App            *AppConfig      `mapstructure:"app"`
	Log            *LogConfig      `mapstructure:"logger"`
	Network        *NetworkConfig  `mapstructure:"network"`
	Database       *DatabaseConfig `mapstructure:"database"`
	GlobalMQConfig *GlobalMQConfig `mapstructure:"global_mq"`
	GrpcGuidConfig *GrpcGuidConfig `mapstructure:"grpc-guid"`
}

// Config 全局配置结构（合并后的完整配置）
type Config struct {
	Base            *BaseConfig     `mapstructure:"base"`
	App             *AppConfig      `mapstructure:"app"`
	Log             *LogConfig      `mapstructure:"logger"`
	Network         *NetworkConfig  `mapstructure:"network"`
	Database        *DatabaseConfig `mapstructure:"database"`
	GlobalMQConfig  *GlobalMQConfig `mapstructure:"global_mq"`
	FeiZhuSdkConfig *SDKGameConfig  `mapstructure:"feizhu-game"`
	WeiXinSdkConfig *SDKGameConfig  `mapstructure:"weixin-game"`
	DouyinSdkConfig *SDKGameConfig  `mapstructure:"douyin-game"`
	GrpcGuidConfig  *GrpcGuidConfig `mapstructure:"grpc-guid"`
}

type BaseConfig struct {
	XxlUrl string `mapstructure:"xxlUrl"`
	XxlEnv string `mapstructure:"xxlEnv"`
	XxlKey string `mapstructure:"xxlKey"`
	XxlApp string `mapstructure:"xxlApp"`
}

// AppConfig 应用配置
type AppConfig struct {
	Name         string `mapstructure:"name"`
	Version      string `mapstructure:"version"`
	Env          string `mapstructure:"env"`
	Debug        bool   `mapstructure:"debug"`
	ConfigPath   string `mapstructure:"configPath"`
	IsAutoUpdate bool   `mapstructure:"isAutoUpdate"`
}

// LogConfig 日志配置
type LogConfig struct {
	Level      string `mapstructure:"level"`
	Path       string `mapstructure:"path"`
	MaxSize    int    `mapstructure:"max_size"`
	MaxBackups int    `mapstructure:"max_backups"`
	MaxAge     int    `mapstructure:"max_age"`
	Compress   bool   `mapstructure:"compress"`
}

// NetworkConfig 网络配置
type NetworkConfig struct {
	TokenSignStr string      `mapstructure:"token_sign_str"`
	TCP          *TCPConfig  `mapstructure:"tcp"`
	HTTP         *HTTPConfig `mapstructure:"http"`
}

// HTTPConfig HTTP配置
type HTTPConfig struct {
	Enabled        bool     `mapstructure:"enabled"`
	Host           string   `mapstructure:"host"`
	StartPort      int      `mapstructure:"start_port"`
	EndPort        int      `mapstructure:"end_port"`
	Port           int      `mapstructure:"port"`
	ReadTimeout    int64    `mapstructure:"read_timeout"`     // 读取超时
	WriteTimeout   int64    `mapstructure:"write_timeout"`    // 写入超时
	IdleTimeout    int64    `mapstructure:"idle_timeout"`     // 空闲超时
	MaxHeaderBytes int      `mapstructure:"max_header_bytes"` // 最大头部字节数
	EnableTLS      bool     `mapstructure:"enable_tls"`       // 是否启用TLS
	CertFile       string   `mapstructure:"cert_file"`        // 证书文件路径
	KeyFile        string   `mapstructure:"key_file"`         // 私钥文件路径
	EnableCORS     bool     `mapstructure:"enable_cors"`      // 是否启用CORS
	CORSOrigins    []string `mapstructure:"cors_origins"`     // CORS允许的源
	EnableMetrics  bool     `mapstructure:"enable_metrics"`   // 是否启用指标收集
	MetricsPath    string   `mapstructure:"metrics_path"`     // 指标路径
	EnablePprof    bool     `mapstructure:"enable_pprof"`     // 是否启用pprof
	PprofPrefix    string   `mapstructure:"pprof_prefix"`     // pprof路径前缀
}

// TCPConfig TCP配置
type TCPConfig struct {
	Enabled      bool   `mapstructure:"enabled"`
	Host         string `mapstructure:"host"`
	StartPort    int    `mapstructure:"start_port"`
	EndPort      int    `mapstructure:"end_port"`
	Port         int    `mapstructure:"port"`
	MaxConn      int    `mapstructure:"max_conn"`
	ReadTimeout  int64  `mapstructure:"read_timeout"`
	WriteTimeout int64  `yaml:"write_timeout"`
	PingTimeOut  int64  `mapstructure:"ping_time_out"`
}

// DatabaseConfig 数据库配置
type DatabaseConfig struct {
	MongoDB *MongoDBConfig `mapstructure:"mongodb"`
}

// GlobalMQConfig 全局MQ配置
type GlobalMQConfig struct {
	Enabled    bool `mapstructure:"enabled"`
	MaxWorkers int  `mapstructure:"max_workers"`
	FrameRate  int  `mapstructure:"frame_rate"`
	QueueSize  int  `mapstructure:"queue_size"`
}

// MongoDBConfig MongoDB配置
type MongoDBConfig struct {
	Enabled   bool          `mapstructure:"enabled"`
	URI       string        `mapstructure:"uri"`
	Database  string        `mapstructure:"database"`
	QueueSize int           `mapstructure:"queue_size"`
	BatchSize int           `mapstructure:"batch_size"`
	Timeout   time.Duration `mapstructure:"timeout"`
}

type SDKGameConfig struct {
	AppID          string `mapstructure:"app-id" json:"app-id" yaml:"app-id"`                            // 抖音小游戏AppID
	AppSecret      string `mapstructure:"app-secret" json:"app-secret" yaml:"app-secret"`                // 抖音小游戏AppSecret
	ServerDomain   string `mapstructure:"server-domain" json:"server-domain" yaml:"server-domain"`       // 服务器域名
	AdUnitID       string `mapstructure:"ad-unit-id" json:"ad-unit-id" yaml:"ad-unit-id"`                // 广告单元ID
	CallbackSecret string `mapstructure:"callback-secret" json:"callback-secret" yaml:"callback-secret"` // 回调签名密钥
}

type GrpcGuidConfig struct {
	Enabled   bool   `mapstructure:"enabled"`
	Address   string `mapstructure:"address"`
	ApiSecret string `mapstructure:"api-secret"`
	Project   string `mapstructure:"project"`
	Env       string `mapstructure:"env"`
}

// 配置管理器
type ConfigManager struct {
	config *Config
	mu     sync.RWMutex
}

var (
	globalConfig *ConfigManager
	once         sync.Once
)

// Init 初始化配置
func Init(config *Config) error {
	once.Do(func() {
		globalConfig = &ConfigManager{config: config}
	})
	return globalConfig.validate()
}

// Get 获取配置
func Get() *Config {
	globalConfig.mu.RLock()
	defer globalConfig.mu.RUnlock()
	if globalConfig == nil {
		return &Config{}
	}
	return globalConfig.config
}

// 验证配置
func (cm *ConfigManager) validate() error {
	if cm.config == nil {
		return errors.New("config is nil")
	}
	if cm.config.App == nil {
		cm.config.App = &AppConfig{
			Name:    "yaice",
			Version: "1.0.0",
			Env:     "development",
		}
	}
	if cm.config.Log == nil {
		cm.config.Log = &LogConfig{
			Level:    "info",
			Path:     "./logs",
			MaxSize:  100,
			MaxAge:   7,
			Compress: true,
		}
	}
	if err := cm.validateNetworkConfig(); err != nil {
		return err
	}
	if err := cm.validateDatabaseConfig(); err != nil {
		return err
	}
	return nil
}

// validateNetworkConfig 验证网络配置
func (cm *ConfigManager) validateNetworkConfig() error {
	cfg := cm.config.Network
	// 验证TCP配置
	if cfg.TCP != nil && cfg.TCP.Enabled {
		if cfg.TCP.StartPort <= 0 || cfg.TCP.EndPort <= 0 || cfg.TCP.EndPort > 65535 || cfg.TCP.StartPort > 65535 {
			return errors.New("tcp_net port must be between 1 and 65535")
		}
		if cfg.TCP.MaxConn <= 0 {
			cfg.TCP.MaxConn = 10000
		}
	}

	// 验证HTTP配置
	if cfg.HTTP != nil && cfg.HTTP.Enabled {
		if cfg.HTTP.StartPort <= 0 || cfg.HTTP.EndPort <= 0 || cfg.HTTP.EndPort > 65535 || cfg.HTTP.StartPort > 65535 {
			return errors.New("http port must be between 1 and 65535")
		}
		if cfg.HTTP.EnableTLS {
			if cfg.HTTP.CertFile == "" || cfg.HTTP.KeyFile == "" {
				return errors.New("cert_file and key_file are required when enable_tls is true")
			}
		}
	}
	return nil
}

// validateDatabaseConfig 验证数据库配置
func (cm *ConfigManager) validateDatabaseConfig() error {
	cfg := cm.config.Database

	// 验证MongoDB配置
	if cfg.MongoDB != nil && cfg.MongoDB.Enabled {
		if cfg.MongoDB.URI == "" {
			return errors.New("mongodb uri is required")
		}
		if cfg.MongoDB.Database == "" {
			return errors.New("mongodb database name is required")
		}
		if cfg.MongoDB.QueueSize == 0 {
			cfg.MongoDB.QueueSize = 3000
		}
		if cfg.MongoDB.Timeout == 0 {
			cfg.MongoDB.Timeout = 10 * time.Second
		}
	}
	return nil
}
