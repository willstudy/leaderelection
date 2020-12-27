## 简介
借鉴k8s leaderelection和etcd实现了一个leader选举机制。与k8s leader election相比:

- 优点: 依赖很少，但够用。
- 缺点: 功能没有k8s election的强大

## 使用方法
```
import "github.com/willstudy/leaderelection/leader"
...

leCfg := &leader.LeaderElectionConfig{
    LeaseDuration: 10,
    Callbacks: leader.LeaderCallbacks{
        OnStartedLeading: func(ctx context.Context) {
            // 主程序初始化
            ...
            // main.run()
        },
        OnStoppedLeading: func() {
            log.Notice("good bye")
        },
        OnNewLeader: func() {
            log.Notice("now leader is: %s", leader.G_Leader.GetLeaderName())
        },
    },
    Client:    etcdCli,
    Log:       log,
    LeaderKey: "test_leader_path",
    IpPort:    hostname + ":" + port,
}
leader.G_Leader, err = leader.NewLeaderElector(leCfg)
if err != nil {
    return err
}
leader.G_Leader.Run(context.TODO())
```
