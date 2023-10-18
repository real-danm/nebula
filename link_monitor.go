package nebula

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/sirupsen/logrus"
	"github.com/slackhq/nebula/iputil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	protoinfra "github.com/livekit/protocol/infra"
)

type limiter struct {
	mu            sync.RWMutex
	lastResetTime time.Time
	attempts      int
}

type region struct {
	name           string
	numDataPoints  int
	cidr           *net.IPNet
	lock           sync.RWMutex
	rttEmaValue    float64 // Exponential moving average
	jitterEmaValue float64
}

type LinkMonitorRegionConfig struct {
	Name string `yaml:"name"`
	Cidr string `yaml:"cidr"`
}

type LinkMonitorConfig struct {
	Enabled       bool                      `yaml:"enabled"`
	Host          string                    `yaml:"host"`
	Port          string                    `yaml:"port"`
	CurrentRegion string                    `yaml:"current_region"`
	Alpha         float64                   `yaml:"alpha"`
	MaxRtt        float64                   `yaml:"max_rtt_microseconds"`
	MaxJitter     float64                   `yaml:"max_jitter_microseconds"`
	MinDataPoints int                       `yaml:"min_data_points"`
	ChurnFailures int                       `yaml:"churn_failures"`
	ChurnInterval time.Duration             `yaml:"churn_interval"`
	Regions       []LinkMonitorRegionConfig `yaml:"regions"`
}

type LinkMonitor struct {
	lock     sync.RWMutex
	l        *logrus.Logger
	churnMap *lru.Cache[string, *limiter]
	config   LinkMonitorConfig
	enabled  bool

	regionMap             map[string]*region
	linkMonitorConnection *grpc.ClientConn
	linkMonitorClient     protoinfra.LinkClient
	linkMessageChan       chan *protoinfra.WatchLocalLinksResponse
}

func NewLinkMonitorConfig(config map[interface{}]interface{}) LinkMonitorConfig {
	var linkMonitorConfig LinkMonitorConfig

	if val, ok := config["link_monitor"]; ok {
		if lmMap, isOk := val.(map[interface{}]interface{}); isOk {
			linkMonitorConfig.Enabled = lmMap["enabled"].(bool)
			linkMonitorConfig.Host = lmMap["host"].(string)
			linkMonitorConfig.Port = lmMap["port"].(string)
			linkMonitorConfig.CurrentRegion = lmMap["current_region"].(string)
			linkMonitorConfig.Alpha = lmMap["alpha"].(float64)
			linkMonitorConfig.MaxRtt = lmMap["max_rtt_microseconds"].(float64)
			linkMonitorConfig.MaxJitter = lmMap["max_jitter_microseconds"].(float64)
			linkMonitorConfig.MinDataPoints = lmMap["min_data_points"].(int)
			linkMonitorConfig.ChurnFailures = lmMap["churn_failures"].(int)
			linkMonitorConfig.ChurnInterval, _ = time.ParseDuration(lmMap["churn_interval"].(string))

			regions := lmMap["regions"].([]interface{})
			for _, r := range regions {
				regionMap := r.(map[interface{}]interface{})
				linkMonitorConfig.Regions = append(linkMonitorConfig.Regions, LinkMonitorRegionConfig{
					Name: regionMap["name"].(string),
					Cidr: regionMap["cidr"].(string),
				})
			}
		}
	}

	return linkMonitorConfig
}

func NewLinkMonitor(ctx context.Context, l *logrus.Logger, config map[interface{}]interface{}) *LinkMonitor {
	linkMonitorConfig := NewLinkMonitorConfig(config)

	if !linkMonitorConfig.Enabled {
		return &LinkMonitor{
			enabled: false,
		}
	}

	limiterMap, err := lru.New[string, *limiter](1000)
	if err != nil {
		l.Error("failed to create churn map", err)
		return &LinkMonitor{
			enabled: false,
		}
	}

	regionMap := make(map[string]*region)
	for _, r := range linkMonitorConfig.Regions {
		_, cidr, err := net.ParseCIDR(r.Cidr)
		if err != nil {
			l.Error("failed to parse region cidr", err)
			continue
		}
		regionMap[r.Name] = &region{
			name:          r.Name,
			numDataPoints: 0,
			cidr:          cidr,
		}
	}

	linkMonitor := &LinkMonitor{
		lock:            sync.RWMutex{},
		l:               l,
		churnMap:        limiterMap,
		regionMap:       regionMap,
		linkMessageChan: make(chan *protoinfra.WatchLocalLinksResponse, 100),
		config:          linkMonitorConfig,
		enabled:         linkMonitorConfig.Enabled,
	}

	return linkMonitor
}

/*
* Check() guards nebula timed attempts to roam back to a direct connection
* check if vpnIp is in churn map and has exceeded the churn settings
* check the region map for link monitor stats based on the vpnIp
* if either fail return false
 */
func (l *LinkMonitor) Check(vpnIp iputil.VpnIp) bool {
	if !l.config.Enabled {
		return true
	}

	current, ok := l.churnMap.Get(vpnIp.String())
	if ok {
		current.mu.RLock()
		defer current.mu.RUnlock()

		if time.Since(current.lastResetTime) < l.config.ChurnInterval {
			if current.attempts > l.config.ChurnFailures {
				return false
			}
		}
	}

	pass, err := l.checkRegionalStats(vpnIp)
	if err != nil {
		l.l.Error("failed to check regional stats", err)
		return true
	}

	return pass
}

/*
* Attempt() guards receiving direct handshake requests from a specific vpnIp
* check if vpnIp is in churn map and has exceeded the churn settings
* increment the churn map for this vpnIp
* check the region map for link monitor stats based on the vpnIp
* if either fail return false
 */
func (l *LinkMonitor) Attempt(vpnIp iputil.VpnIp) bool {
	if !l.enabled {
		return true
	}

	current, ok := l.churnMap.Get(vpnIp.String())
	if !ok {
		current = &limiter{
			lastResetTime: time.Now(),
			attempts:      0,
		}
	}

	allow := true
	current.mu.Lock()
	defer current.mu.Unlock()
	if time.Since(current.lastResetTime) < l.config.ChurnInterval {
		current.attempts++
		if current.attempts > l.config.ChurnFailures {
			allow = false
		}
	} else {
		current.lastResetTime = time.Now()
		current.attempts = 1
	}

	l.lock.Lock()
	l.churnMap.Add(vpnIp.String(), current)
	l.lock.Unlock()

	if !allow {
		return false
	}

	pass, err := l.checkRegionalStats(vpnIp)
	if err != nil {
		l.l.Error("failed to check regional stats", err)
		return true
	}

	return pass
}

func (l *LinkMonitor) connect() error {
	l.lock.Lock()
	defer l.lock.Unlock()

	if l.linkMonitorConnection != nil && l.linkMonitorConnection.GetState() != connectivity.Shutdown {
		return nil
	}

	err := l.linkMonitorConnection.Close()
	if err != nil {
		l.l.Warn("failed to close link monitor connection, continuing", err)
	}

	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", l.config.Host, l.config.Port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	l.linkMonitorConnection = conn

	client := protoinfra.NewLinkClient(l.linkMonitorConnection)
	l.linkMonitorClient = client
	return nil
}

func (l *LinkMonitor) Start(ctx context.Context) {
	err := l.connect()
	if err != nil {
		l.l.Errorln("failed to connect to link monitor", err)
	}

	go l.consumeLinkMessage(ctx)
	go l.watchLink(ctx)
}

func (l *LinkMonitor) watchLink(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if l.linkMonitorClient == nil || l.linkMonitorConnection.GetState() != connectivity.Ready {
				l.l.Warn("link monitor not ready, attempting reconnect")
				l.resetRegionalStats()
				err := l.connect()
				if err != nil {
					l.l.Errorln("failed to connect to link monitor", err)
				}
				continue
			}

			val, err := l.linkMonitorClient.WatchLocalLinks(ctx, &protoinfra.WatchLocalLinksRequest{}, grpc.WaitForReady(true))
			if err != nil {
				l.l.Errorln("failed to start watch local links", err)
				continue
			}
			msg, err := val.Recv()
			if err != nil {
				l.l.Errorln("failed to receive watch local links", err)

			}
			l.linkMessageChan <- msg
		}
	}
}

func (l *LinkMonitor) consumeLinkMessage(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-l.linkMessageChan:
			if msg.RemoteRegion == l.config.CurrentRegion {
				continue
			}
			region, ok := l.regionMap[msg.RemoteRegion]
			if !ok {
				l.l.Warnf("received unknown region %s", msg.RemoteRegion)
				continue
			}

			region.lock.Lock()
			region.numDataPoints++
			region.rttEmaValue = (l.config.Alpha * float64(msg.Rtt)) + ((1 - l.config.Alpha) * region.rttEmaValue)
			region.jitterEmaValue = (l.config.Alpha * float64(msg.Jitter)) + ((1 - l.config.Alpha) * region.jitterEmaValue)
			region.lock.Unlock()
		}
	}
}

func (l *LinkMonitor) resetRegionalStats() {
	for _, region := range l.regionMap {
		region.lock.Lock()
		region.numDataPoints = 0
		region.rttEmaValue = 0
		region.jitterEmaValue = 0
		region.lock.Unlock()
	}
}

func (l *LinkMonitor) getRegionalStats(vpnIp iputil.VpnIp) *region {
	for _, region := range l.regionMap {
		if region.cidr.Contains(vpnIp.ToIP()) {
			return region
		}
	}
	return nil
}

func (l *LinkMonitor) checkRegionalStats(vpnIp iputil.VpnIp) (bool, error) {
	regionalStats := l.getRegionalStats(vpnIp)
	if regionalStats == nil {
		return true, fmt.Errorf("failed to find regional stats for vpnIp %s", vpnIp)
	}

	regionalStats.lock.RLock()
	defer regionalStats.lock.RUnlock()

	if regionalStats.numDataPoints < l.config.MinDataPoints {
		l.l.WithFields(logrus.Fields{
			"vpnIp":  vpnIp,
			"region": regionalStats.name,
		}).Info("not enough data points for region")
		return true, nil
	}

	if regionalStats.rttEmaValue > l.config.MaxRtt || regionalStats.jitterEmaValue > l.config.MaxJitter {
		l.l.WithFields(logrus.Fields{
			"vpnIp":  vpnIp,
			"rtt":    regionalStats.rttEmaValue,
			"jitter": regionalStats.jitterEmaValue,
			"region": regionalStats.name,
		}).Warn("region has high rtt or jitter")
		return false, nil
	}

	return true, nil
}

func (l *LinkMonitor) Stop() {
	if l.linkMonitorConnection != nil {
		l.linkMonitorConnection.Close()
	}

	close(l.linkMessageChan)
}
