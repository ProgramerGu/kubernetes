package ipam

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"k8s.io/klog"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/pkg/transport"
	v1 "k8s.io/api/core/v1"
)

type EtcdConfig struct {
	EtcdURL           string `json:"etcdURL"`
	EtcdCertFile      string `json:"etcdCertFile"`
	EtcdKeyFile       string `json:"etcdKeyFile"`
	EtcdTrustedCAFile string `json:"etcdTrustedCAFile"`
	EtcdPrefix        string `json:"etcdPrefix"`
}

type PodInfo struct {
	IgnoreunKnown bool   `json:"ignoreunknown"`
	PodName       string `json:"podName"`
	PodNamespace  string `json:"podNamespace"`
	ContainerID   string `json:"containerId"`
	NodeName      string `json:"nodeName"`
	PodIP         string `json:"podIp"`
}

// Store is a simple disk-backed store that creates one file per IP
// address in a given directory. The contents of the file are the container ID.
type IpamConfig struct {
	EtcdClient    *clientv3.Client
	EtcdKeyPrefix string
}

var Ipam = &EtcdConfig{}
var IpamClient = &IpamConfig{}
var IsInit = 0

//New Ipam Client
func NewIpamClient(etcdConfig *EtcdConfig) (*IpamConfig, error) {
	klog.V(4).Infof("hostbind NewIpamClient etcdConfig %v", etcdConfig)
	etcdClient, err := connectStore(etcdConfig)
	if err != nil {
		klog.V(4).Infof("hostbind NewIpamClient err %s", err)
		return nil, err
	}
	ipamConfig := &IpamConfig{
		EtcdClient:    etcdClient,
		EtcdKeyPrefix: etcdConfig.EtcdPrefix,
	}
	klog.V(4).Infof("hostbind NewIpamClient ipamConfig %v", ipamConfig)
	return ipamConfig, nil
}

//Init Ipam Config
func Config() *IpamConfig {
	klog.V(4).Infof("hostbind Config")
	var err error
	//if IpamClient.EtcdClient == nil || IpamClient.EtcdKeyPrefix == "" {
	if IsInit == 0 {
		IpamClient, err = NewIpamClient(Ipam)
		if err != nil {
			klog.V(4).Infof("hostbind NewIpamClient err %s", err)
		}
		IsInit = 1
	}
	return IpamClient
}

func connectStore(etcdConfig *EtcdConfig) (*clientv3.Client, error) {

	var etcdClient *clientv3.Client
	var err error
	if etcdConfig.EtcdURL == "" || etcdConfig.EtcdCertFile == "" || etcdConfig.EtcdKeyFile == "" || etcdConfig.EtcdTrustedCAFile == "" {
		return nil, errors.New(fmt.Sprintf("hostbind ipam etcd config %v get wrong", etcdConfig))
	}
	if strings.HasPrefix(etcdConfig.EtcdURL, "https") {
		klog.V(4).Infof("hostbind https")
		etcdClient, err = connectWithTLS(etcdConfig.EtcdURL, etcdConfig.EtcdCertFile, etcdConfig.EtcdKeyFile, etcdConfig.EtcdTrustedCAFile)
	} else {
		klog.V(4).Infof("hostbind http")
		etcdClient, err = connectWithoutTLS(etcdConfig.EtcdURL)
	}

	return etcdClient, err
}

/*
   ETCD Related
*/
func connectWithoutTLS(url string) (*clientv3.Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(url, ","),
		DialTimeout: 5 * time.Second,
	})

	return cli, err
}

func connectWithTLS(url, cert, key, trusted string) (*clientv3.Client, error) {
	tlsInfo := transport.TLSInfo{
		CertFile:      cert,
		KeyFile:       key,
		TrustedCAFile: trusted,
	}

	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return nil, err
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   strings.Split(url, ","),
		DialTimeout: 5 * time.Second,
		TLS:         tlsConfig,
	})

	return cli, err
}

//Get pod bind info from etcd by key(podNS+podname) , only use Public IP has bind info
func (ic *IpamConfig) GetBindPodsByNetwork(pod *v1.Pod) (string, error) {
	klog.V(4).Infof("hostbind GetBindPodsByNetwork IpamConfig %v", ic)
	if ic == nil {
		klog.V(4).Infof("ipam etcd client is nil")
		return "", errors.New("ipam etcd client is nil")
	}
	var (
		attempts = 3
		err      error
	)

	key := ic.makePodKey(pod.Name, pod.Namespace)
	klog.V(4).Infof("hostbind get pod binds podname : %s ns : %s key : %s", pod.Name, pod.Namespace, key)
	for i := 0; i < attempts; i++ {
		podBinds, err := ic.getPodInfoByPodKey(key)
		if err == nil {
			return podBinds.NodeName, nil
		}
		klog.V(4).Infof("hostbind get pod binds error: %s", err.Error())
	}
	return "", err

}

func (ic *IpamConfig) makePodKey(podName string, podNS string) string {
	podKey := podNS + "-" + podName
	return (IpamClient.EtcdKeyPrefix + podKey)

}

func (ic *IpamConfig) getPodInfoByPodKey(key string) (*PodInfo, error) {
	ic.Lock()
	defer ic.Unlock()
	var (
		podInfo = PodInfo{}
	)
	podInfoString, id, err := ic.getInfoByKey(key)
	klog.V(4).Infof("hostbind getPodInfoByPodKey podInfoString %s id %d ", podInfoString, id)
	if err != nil {
		klog.V(4).Infof("hostbind getPodInfoByPodKey podInfoString %s id %d error: %s", podInfoString, id, err)
		return nil, err
	}
	if id == -1 {
		klog.V(4).Infof("hostbind pod key has no bind host %s", key)
		return &podInfo, nil
	}
	err = json.Unmarshal(podInfoString, &podInfo)
	klog.V(4).Infof("hostbind getPodInfoByPodKey podInfo %v", &podInfo)
	if err != nil {
		klog.V(4).Infof("hostbind getPodInfoByPodKey podInfo %v error: %s", &podInfo, err)
		return nil, err
	}
	return &podInfo, nil
}

// Get pod by pod key that combine pod name and pod namespace
func (ic *IpamConfig) getInfoByKey(key string) ([]byte, int64, error) {
	resp, err := ic.EtcdClient.Get(context.TODO(), key)
	klog.V(4).Infof("hostbind getInfoByKey resp %v", resp)
	if err != nil {
		klog.V(4).Infof("hostbind getInfoByKey error: %s", err)
		return nil, 0, err
	}
	if resp.Count == 0 {
		klog.V(4).Infof("hostbind getInfoByKey resp.Count: %d", resp.Count)
		return nil, -1, nil
	}

	info := resp.Kvs[0].Value
	klog.V(4).Infof("hostbind getInfoByKey resp.Kvs[0].Value %s id %d", info, resp.Kvs[0].Lease)
	return info, resp.Kvs[0].Lease, nil
}

func (ic *IpamConfig) Close() error {
	// stub we don't need close anything
	return nil
}

func (ic *IpamConfig) Lock() error {
	key := ic.EtcdKeyPrefix + "/lock"

	kv := clientv3.NewKV(ic.EtcdClient)

	retryTimes := 20
	leaseTTL := 10

	getLock := false

	for i := 0; i < retryTimes; i++ {
		// define a lease
		lease := clientv3.NewLease(ic.EtcdClient)
		var leaseResp *clientv3.LeaseGrantResponse
		var err error
		if leaseResp, err = lease.Grant(context.TODO(), int64(leaseTTL)); err != nil {
			return err
		}

		// get leaseId
		leaseId := leaseResp.ID

		// define txn
		txn := kv.Txn(context.TODO())
		txn.If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
			Then(clientv3.OpPut(key, strconv.FormatInt(int64(leaseId), 10), clientv3.WithLease(leaseId))).
			Else(clientv3.OpGet(key))

		// commit txn
		var txnResp *clientv3.TxnResponse
		if txnResp, err = txn.Commit(); err != nil {
			return err
		}

		// return if successed
		if txnResp.Succeeded {
			getLock = true
			break
			// try again
		} else {
			time.Sleep(time.Second * 2)
			continue
		}
	}

	if getLock {
		return nil
	} else {
		return errors.New("Can not get lock")
	}
}

func (ic *IpamConfig) Unlock() error {
	key := ic.EtcdKeyPrefix + "/lock"
	resp, err := ic.EtcdClient.Get(context.TODO(), key)
	if err != nil {
		return err
	}
	if resp.Count > 0 {
		value := string(resp.Kvs[0].Value)
		leaseId, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return err
		}
		lease := clientv3.NewLease(ic.EtcdClient)
		lease.Revoke(context.TODO(), clientv3.LeaseID(leaseId))
	}
	return nil
}
