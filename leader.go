package leaderelection

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/etcd-io/etcd/clientv3"
)

// leader切换的回调函数
type LeaderCallbacks struct {
	OnStartedLeading func(context.Context) // 刚开始是leader时
	OnStoppedLeading func()                // 不再是leader时
	OnNewLeader      func()                // 新leader选举出来时
}

type LeaderElectionConfig struct {
	LeaseDuration int64             // leader锁有效期(s)
	Callbacks     LeaderCallbacks   // 回调函数
	Client        *clientv3.Client // etcd client, TODO: 搞成接口
	LeaderKey     string
	IpPort        string
}

type LeaderElector struct {
	config     *LeaderElectionConfig
	uniquifier string
	leaderName string
}

var G_Leader *LeaderElector

func NewLeaderElector(cfg *LeaderElectionConfig) (*LeaderElector, error) {
	if cfg == nil {
		return nil, fmt.Errorf("LeaderElectionConfig is nil")
	}
	if cfg.LeaseDuration < 5 {
		return nil, fmt.Errorf("LeaderElectionConfig.LeaseDuration required >= 5")
	}
	if cfg.Callbacks.OnStartedLeading == nil || cfg.Callbacks.OnStoppedLeading == nil {
		return nil, fmt.Errorf("LeaderElectionConfig.Callback [OnStartedLeading, OnStoppedLeading] is required")
	}
	if cfg.Client == nil {
		return nil, fmt.Errorf("LeaderElectionConfig.Client is nil")
	}
	return &LeaderElector{
		config:     cfg,
		uniquifier: cfg.IpPort,
	}, nil
}

func (le *LeaderElector) heartbeat() error {
	leaseId, err := le.config.Client.GrantLease(le.config.LeaseDuration)
	if err != nil {
		le.config.Log.Error("LeaderElector GrantLease failed, err:%s", err.Error())
		return err
	}
	if _, err = le.config.Client.Txn(context.TODO()).
		If(clientv3.Compare(clientv3.Value(le.config.LeaderKey), "=", le.uniquifier)).
		Then(clientv3.OpPut(le.config.LeaderKey, le.uniquifier, clientv3.WithLease(leaseId))).
		Commit(); err != nil {
		le.config.Log.Error("LeaderElector heartbeat failed, err: %s", err.Error())
		return err
	}
	return nil
}

func (le *LeaderElector) tryBeLeader(ctx context.Context) bool {
	leaseId, err := le.config.Client.GrantLease(le.config.LeaseDuration)
	if err != nil {
		le.config.Log.Error("LeaderElector GrantLease failed, err:%s", err.Error())
		return false
	}
	if _, err = le.config.Client.Txn(context.TODO()).
		If(clientv3.Compare(clientv3.Version(le.config.LeaderKey), "=", 0)). // TODO: 判断条件需要细化下
		Then(clientv3.OpPut(le.config.LeaderKey, le.uniquifier, clientv3.WithLease(leaseId))).
		Commit(); err != nil {
		le.config.Log.Error("LeaderElector try to be a leader failed, err: %s", err.Error())
		return false
	}
	le.leaderName = le.uniquifier
	le.config.Log.Notice("LeaderElector be a leader, leaderName[%s]", le.leaderName)
	go le.config.Callbacks.OnNewLeader()
	return true
}

// try to be a leader
func (le *LeaderElector) acquire(ctx context.Context) bool {
	for {
		time.Sleep(1 * time.Second)
		// 1. get leader name from etcd
		resp, err := le.config.Client.Get(context.TODO(), le.config.LeaderKey, clientv3.WithLimit(1))
		if err != nil {
			le.config.Log.Error("LeaderElector get leaderKey failed, err:[%s]", err.Error())
			continue
		}
		// 2. leader not existed, try to be a leader
		if len(resp.Kvs) == 0 {
			le.config.Log.Notice("LeaderElector leaderKey not existed, try to be a leader")
			if !le.tryBeLeader(ctx) {
				continue
			}
			break
		} else {
			le.leaderName = string(resp.Kvs[0].Value[:])
		}
		// 2. if self is leader
		if le.leaderName == le.uniquifier {
			le.config.Log.Notice("LeaderElector resume, leaderName[%s]", le.leaderName)
			break
		}
	}
	return true
}

// need exit if self is running & leader != self.uniquifier
func (le *LeaderElector) watch(ctx context.Context) {
	for {
		time.Sleep(1 * time.Second)
		// 1. get leader name from etcd
		resp, err := le.config.Client.Get(context.TODO(), le.config.LeaderKey, clientv3.WithLimit(1))
		if err != nil {
			le.config.Log.Error("LeaderElector get leaderKey failed, err:[%s]", err.Error())
			continue
		}
		// 2. leader not existed, try to be a leader
		if len(resp.Kvs) == 0 {
			le.config.Log.Notice("LeaderElector leaderKey not existed, try to be a leader")
			if !le.tryBeLeader(ctx) {
				continue
			}
		} else {
			le.leaderName = string(resp.Kvs[0].Value[:])
		}
		// 2. if self is not the leader
		if le.leaderName != le.uniquifier {
			le.config.Log.Notice("LeaderElector uniquifier[%s] != leaderName[%s], but running, need to exit",
				le.uniquifier, le.leaderName)
			break
		}
		// 3. update heartbeat
		le.heartbeat()
	}
	return
}

func (le *LeaderElector) graceExit() {
	if le.leaderName != le.uniquifier {
		return
	}
	if _, err := le.config.Client.Txn(context.TODO()).
		If(clientv3.Compare(clientv3.Value(le.config.LeaderKey), "=", le.leaderName)).
		Then(clientv3.OpDelete(le.config.LeaderKey)).
		Commit(); err != nil {
		le.config.Log.Error("LeaderElector release failed, err: %s", err.Error())
	}
	return
}

// 获取数据迁移锁
func (le *LeaderElector) acquireSyncLock(appId string) (int, error) {
	fields := strings.Split(appId, ".")
	idc := fields[len(fields)-1]
	lockSyncPrefix := define.G_lock_sync_prefix + "_" + idc
	keys, err := le.config.Client.GetKeysWithPrefix(lockSyncPrefix)
	if err != nil {
		return len(keys), err
	}
	found := false
	for _, key := range keys {
		if strings.HasSuffix(key, appId) {
			found = true
			break
		}
	}
	// 该app已经获取锁
	if found {
		return len(keys), nil
	}
	if len(keys) >= define.G_lock_sync_num {
		return 0, fmt.Errorf("sync app locked > %d", define.G_lock_sync_num)
	}
	// try to grant lease
	resp, err := le.config.Client.Grant(context.TODO(), define.G_lock_ttl)
	if err != nil {
		return len(keys), err
	}
    // TODO: try to acquire sync lock (需要加实例ip?)
	lockKey := fmt.Sprintf("%s_%s", lockSyncPrefix, appId)
	if _, err = le.config.Client.GetClient().Put(context.TODO(), lockKey, "null", clientv3.WithLease(resp.ID)); err != nil {
		return len(keys), err
	}
	return len(keys) + 1, nil
}

func (le *LeaderElector) startHttpListen() {
	// 1. add http handler
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "Hello, I am ok, leader is %s", le.GetLeaderName())
	})
	http.Handle("/metrics", promhttp.Handler())
    // 2. add listen port
	fields := strings.Split(le.config.IpPort, ":")
	listenAddr := ":" + fields[1]
	if err := http.ListenAndServe(listenAddr, nil); err != nil {
		le.config.Log.Error("LeaderElector listen failed, addr:%s err:%s", listenAddr, err.Error())
		time.Sleep(time.Second)
		os.Exit(-1)
	}
	le.config.Log.Notice("LeaderElector listen addr: %s", listenAddr)
}

func (le *LeaderElector) GetLeaderName() string {
	return le.leaderName
}

func (le *LeaderElector) Run(ctx context.Context) {
	defer func() {
		le.config.Callbacks.OnStoppedLeading()
		le.graceExit()
	}()
	go le.startHttpListen()
	if !le.acquire(ctx) {
		return
	}
	go le.config.Callbacks.OnStartedLeading(ctx)
	le.watch(ctx)
}
