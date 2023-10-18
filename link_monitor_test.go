package nebula

import (
	"context"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/slackhq/nebula/iputil"
	"github.com/stretchr/testify/assert"
)

var inputConfig = map[interface{}]interface{}{
	"link_monitor": map[interface{}]interface{}{
		"enabled":                 true,
		"host":                    "localhost",
		"port":                    "9999",
		"current_region":          "region1",
		"alpha":                   0.5,
		"min_data_points":         10,
		"max_rtt_microseconds":    300000.0,
		"max_jitter_microseconds": 50000.0,
		"churn_failures":          1,
		"churn_interval":          "10m",
		"regions": []interface{}{
			map[interface{}]interface{}{
				"name": "region1",
				"cidr": "10.1.0.0/16",
			},
			map[interface{}]interface{}{
				"name": "region2",
				"cidr": "10.2.0.0/16",
			},
			map[interface{}]interface{}{
				"name": "region3",
				"cidr": "10.3.0.0/16",
			},
		},
	},
}

func TestNewLinkMonitorConfig(t *testing.T) {
	expectedConfig := LinkMonitorConfig{
		Enabled:       true,
		Host:          "localhost",
		Port:          "9999",
		CurrentRegion: "region1",
		Alpha:         0.5,
		MinDataPoints: 10,
		MaxRtt:        300000.0,
		MaxJitter:     50000.0,
		ChurnFailures: 1,
		ChurnInterval: 10 * time.Minute,
		Regions: []LinkMonitorRegionConfig{
			{Name: "region1", Cidr: "10.1.0.0/16"},
			{Name: "region2", Cidr: "10.2.0.0/16"},
			{Name: "region3", Cidr: "10.3.0.0/16"},
		},
	}

	actualConfig := NewLinkMonitorConfig(inputConfig)
	if !reflect.DeepEqual(expectedConfig, actualConfig) {
		t.Fatalf("expected %v but got %v", expectedConfig, actualConfig)
	}
}

func mockLinkMonitor() *LinkMonitor {
	logger := logrus.New()
	ctx := context.TODO()
	lm := NewLinkMonitor(ctx, logger, inputConfig)
	return lm
}

func TestNewLinkMonitor(t *testing.T) {
	lm := mockLinkMonitor()
	assert.NotNil(t, lm, "Expected LinkMonitor to be not nil")
}

func TestDisabledLinkMonitor(t *testing.T) {
	lm := mockLinkMonitor()
	lm.enabled = false
	vpnIp := iputil.Ip2VpnIp(net.ParseIP("10.1.0.1"))
	assert.True(t, lm.Check(vpnIp), "Expected Check to return true")
	assert.True(t, lm.Attempt(vpnIp), "Expected Attempt to return true")
}

func TestLinkMonitorCheckEmpty(t *testing.T) {
	lm := mockLinkMonitor()
	vpnIp := iputil.Ip2VpnIp(net.ParseIP("10.1.0.1"))
	assert.True(t, lm.Check(vpnIp), "Expected Check to return true")
	assert.True(t, lm.Attempt(vpnIp), "Expected Attempt to return true")
}

func TestLinkMonitorAttempt(t *testing.T) {
	lm := mockLinkMonitor()
	vpnIp := iputil.Ip2VpnIp(net.ParseIP("10.2.0.1"))

	assert.True(t, lm.Attempt(vpnIp), "Expected first Attempt to return true")
	for i := 0; i < lm.config.ChurnFailures+1; i++ {
		lm.Attempt(vpnIp)
	}
	assert.False(t, lm.Attempt(vpnIp), "Expected Attempt after multiple tries to return false")
	assert.False(t, lm.Check(vpnIp), "Expected Check to return false")
}

func startDeadServer(addr string) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		conn.Close()
	}
}

func TestCheckAndAttemptWithDeadEndpoint(t *testing.T) {
	addr := "localhost:9999"

	go startDeadServer(addr)
	defer net.Dial("tcp", addr)

	lm := mockLinkMonitor()

	timeout := time.After(2 * time.Second)
	done := make(chan bool)

	go func() {
		defer close(done)
		vpnIp := iputil.Ip2VpnIp(net.ParseIP("10.1.0.1"))
		assert.True(t, lm.Check(vpnIp), "Expected Check to return true")
		assert.True(t, lm.Attempt(vpnIp), "Expected Attempt to return true")
	}()

	select {
	case <-timeout:
		t.Fatal("Expected Check and Attempt to return before timeout")
	case <-done:
		// successs
	}
}

func TestFailureDueToRtt(t *testing.T) {
	lm := mockLinkMonitor()
	vpnIp := iputil.Ip2VpnIp(net.ParseIP("10.1.0.1"))

	missingRegionIp := iputil.Ip2VpnIp(net.ParseIP("10.15.0.1"))
	successfulRegionIp := iputil.Ip2VpnIp(net.ParseIP("10.2.0.1"))

	_, cidr, err := net.ParseCIDR("10.1.0.0/16")
	if err != nil {
		t.Fatal(err)
	}
	lm.regionMap["region1"] = &region{
		cidr:           cidr,
		numDataPoints:  1,
		rttEmaValue:    1000000.0,
		jitterEmaValue: 0.0,
	}

	_, successfulRegionCidr, err := net.ParseCIDR("10.2.0.0/16")
	if err != nil {
		t.Fatal(err)
	}
	lm.regionMap["region2"] = &region{
		cidr:           successfulRegionCidr,
		numDataPoints:  15,
		rttEmaValue:    0.0,
		jitterEmaValue: 25000.0,
	}

	// too few datapoints should return true
	assert.True(t, lm.Check(vpnIp), "Expected Check to return true")
	assert.True(t, lm.Attempt(vpnIp), "Expected Attempt to return true")
	assert.True(t, lm.Check(successfulRegionIp), "Expected Check to return true")
	assert.True(t, lm.Check(missingRegionIp), "Expected Attempt to return true")

	lm.regionMap["region1"].numDataPoints = 11
	assert.False(t, lm.Check(vpnIp), "Expected Check to return false")
	assert.False(t, lm.Attempt(vpnIp), "Expected Attempt to return false")
	assert.True(t, lm.Check(successfulRegionIp), "Expected Check to return true")
	assert.True(t, lm.Check(missingRegionIp), "Expected Attempt to return true")

	// rtt recover
	lm.regionMap["region1"].rttEmaValue = 100000.0
	// still fail due to churn limiter
	assert.False(t, lm.Check(vpnIp), "Expected Check to return false")
	assert.False(t, lm.Attempt(vpnIp), "Expected Attempt to return false")
	assert.True(t, lm.Check(successfulRegionIp), "Expected Check to return true")
	assert.True(t, lm.Check(missingRegionIp), "Expected Attempt to return true")

	// ignore churn limiter
	lm.config.ChurnFailures = 100
	assert.True(t, lm.Check(vpnIp), "Expected Check to return true")
	assert.True(t, lm.Attempt(vpnIp), "Expected Attempt to return true")
	assert.True(t, lm.Check(successfulRegionIp), "Expected Check to return true")
	assert.True(t, lm.Check(missingRegionIp), "Expected Attempt to return true")
}

func TestFailureDueToJitter(t *testing.T) {
	lm := mockLinkMonitor()
	vpnIp := iputil.Ip2VpnIp(net.ParseIP("10.1.0.1"))

	missingRegionIp := iputil.Ip2VpnIp(net.ParseIP("10.15.0.1"))
	successfulRegionIp := iputil.Ip2VpnIp(net.ParseIP("10.2.0.1"))

	_, cidr, err := net.ParseCIDR("10.1.0.0/16")
	if err != nil {
		t.Fatal(err)
	}
	lm.regionMap["region1"] = &region{
		cidr:           cidr,
		numDataPoints:  1,
		rttEmaValue:    0.0,
		jitterEmaValue: 75000.0,
	}

	_, successfulRegionCidr, err := net.ParseCIDR("10.2.0.0/16")
	if err != nil {
		t.Fatal(err)
	}
	lm.regionMap["region2"] = &region{
		cidr:           successfulRegionCidr,
		numDataPoints:  15,
		rttEmaValue:    0.0,
		jitterEmaValue: 25000.0,
	}

	// too few datapoints should return true
	assert.True(t, lm.Check(vpnIp), "Expected Check to return true")
	assert.True(t, lm.Attempt(vpnIp), "Expected Attempt to return true")
	assert.True(t, lm.Check(successfulRegionIp), "Expected Check to return true")
	assert.True(t, lm.Check(missingRegionIp), "Expected Attempt to return true")

	lm.regionMap["region1"].numDataPoints = 11
	assert.False(t, lm.Check(vpnIp), "Expected Check to return false")
	assert.False(t, lm.Attempt(vpnIp), "Expected Attempt to return false")
	assert.True(t, lm.Check(successfulRegionIp), "Expected Check to return true")
	assert.True(t, lm.Check(missingRegionIp), "Expected Attempt to return true")

	// jitter recover
	lm.regionMap["region1"].jitterEmaValue = 50000.0
	// still fail due to churn limiter
	assert.False(t, lm.Check(vpnIp), "Expected Check to return false")
	assert.False(t, lm.Attempt(vpnIp), "Expected Attempt to return false")
	assert.True(t, lm.Check(successfulRegionIp), "Expected Check to return true")
	assert.True(t, lm.Check(missingRegionIp), "Expected Attempt to return true")

	// ignore churn limiter
	lm.config.ChurnFailures = 100
	assert.True(t, lm.Check(vpnIp), "Expected Check to return true")
	assert.True(t, lm.Attempt(vpnIp), "Expected Attempt to return true")
	assert.True(t, lm.Check(successfulRegionIp), "Expected Check to return true")
	assert.True(t, lm.Check(missingRegionIp), "Expected Attempt to return true")
}
