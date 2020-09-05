package util

import (
	"encoding/json"
	"fmt"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog"
	schedulerinternalcache "k8s.io/kubernetes/pkg/scheduler/internal/cache"
)

var (
	//CSILVMSSD StorageClassName ssd
	CSILVMSSD = "csi-lvm-ssd"
	//CSILVMHDD StorageClassName hdd
	CSILVMHDD = "csi-lvm-hdd"
	//AnnoBoundPod podinfo in pvc annotation
	AnnoBoundPod = "volume.kubernetes.io/bound-pod"
	//AnnoReservation is pod a reservation pod
	AnnoReservation = "bec.baidu.com/reservation"
)

//IsReservationPod is pod need to Reserve
func IsReservationPod(pod *v1.Pod, pVCLister corelisters.PersistentVolumeClaimLister) bool {
	//if pod has pv
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			if volume.PersistentVolumeClaim.ClaimName != "" {
				pvcName := volume.PersistentVolumeClaim.ClaimName
				ns := pod.Namespace
				if strings.Contains(pod.Namespace, "reservation") {
					ns = ns[(strings.Index(ns, "-") + 1):len(ns)]
				}
				pvc, err := pVCLister.PersistentVolumeClaims(ns).Get(pvcName)
				if err != nil {
					continue
				}
				if pvc.DeletionTimestamp == nil && pvc.Status.Phase == v1.ClaimBound && (*pvc.Spec.StorageClassName == CSILVMSSD || *pvc.Spec.StorageClassName == CSILVMHDD) {
					klog.V(4).Infof("pod %s/%s IsReservationPod", pod.Namespace, pod.Name)
					return true
				}
			}
		}
	}
	return false
}
func makeReservationKey(namespace string, name string) string {
	return fmt.Sprintf("reservation-%s/%s", namespace, name)
}

//MakeReservationPod build reservation pod
func MakeReservationPod(pod *v1.Pod) *v1.Pod {
	rpod := pod.DeepCopy()
	rpod.UID = types.UID(makeReservationKey(pod.Namespace, pod.Name))
	rpod.Namespace = fmt.Sprintf("reservation-%s", pod.Namespace)
	if rpod.Annotations != nil {
		rpod.Annotations[AnnoReservation] = "on"
	} else {
		rpod.Annotations = map[string]string{
			AnnoReservation: "on",
		}
	}
	return rpod
}

//GetReservationPod get reservation pod from cache
func GetReservationPod(pod *v1.Pod, schedulerCache schedulerinternalcache.Cache) (*v1.Pod, error) {
	rpod := MakeReservationPod(pod)
	cachePod, err := schedulerCache.GetPod(rpod)
	if err != nil {
		klog.V(4).Infof("GetReservationPod %s/%s error %s", pod.Namespace, pod.Name, err)
		return nil, err
	}
	if reservation := cachePod.Annotations[AnnoReservation] == "on"; reservation {
		klog.V(4).Infof("GetReservationPod %s/%s success", pod.Namespace, pod.Name)
	}
	return cachePod, nil
}

//ReserverInit add reservation pod by pvc list when scheduler start
func ReserverInit(pVCLister corelisters.PersistentVolumeClaimLister, schedulerCache schedulerinternalcache.Cache) error {
	klog.V(3).Info("cache reserver init started")
	defer klog.V(3).Info("cache reserver init finished")
	pvcs, err := pVCLister.List(labels.Everything())
	if err != nil {
		klog.V(4).Infof("ReserverInit list pvc fail %s", err)
		return err
	}
	klog.V(4).Infof("ReserverInit list pvc %d", len(pvcs))
	for _, pvc := range pvcs {
		if pod, needReservation := PVCNeedReservation(pvc, schedulerCache); needReservation {
			klog.V(4).Infof("pvc %s needReservation %t", pvc.Name, needReservation)
			ReservationPod(pod, schedulerCache)
		}
	}
	return nil
}

//Reserver check pvc info and operate reservation pod
func Reserver(pVCLister corelisters.PersistentVolumeClaimLister, schedulerCache schedulerinternalcache.Cache) error {
	klog.V(3).Info("cache reserver started")
	defer klog.V(3).Info("cache reserver finished")

	pvcs, _ := pVCLister.List(labels.Everything())
	klog.V(4).Infof("Reserver list pvc %d", len(pvcs))

	snapshot := schedulerCache.Snapshot()
	if len(snapshot.Nodes) > 0 {
		for _, nodeinfo := range snapshot.Nodes {
			if nodeinfo.Node() == nil {
				continue
			}
			for _, pod := range nodeinfo.Pods() {
				if strings.Contains(string(pod.UID), "reservation") {
					if !IsReservationPod(pod, pVCLister) {
						if err := schedulerCache.RemovePod(pod); err != nil {
							klog.V(4).Infof("Reserver pod %s/%s delete reservation pod fail %s", pod.Namespace, pod.Name, err)
						} else {
							klog.V(4).Infof("Reserver pod %s/%s delete reservation pod success", pod.Namespace, pod.Name)
						}
					} else {
						klog.V(4).Infof("Reserver pod %s/%s no need to remove", pod.Namespace, pod.Name)
					}
				}
			}
		}
	}
	return nil
}

//PVCNeedReservation if pvc need to add reservation pod
func PVCNeedReservation(pvc *v1.PersistentVolumeClaim, schedulerCache schedulerinternalcache.Cache) (*v1.Pod, bool) {
	if pvc.DeletionTimestamp == nil && pvc.Status.Phase == v1.ClaimBound && (*pvc.Spec.StorageClassName == CSILVMSSD || *pvc.Spec.StorageClassName == CSILVMHDD) {
		if pvc.Annotations != nil {
			podString := pvc.Annotations[AnnoBoundPod]
			if podString != "" {
				pod := &v1.Pod{}
				err := json.Unmarshal([]byte(podString), pod)
				if err == nil {
					_, err := schedulerCache.GetPod(pod)
					if err != nil {
						return pod, true
					}
				}
			}
		}
	}
	return nil, false
}

//ReservationPod reservate pod
func ReservationPod(pod *v1.Pod, schedulerCache schedulerinternalcache.Cache) {
	rpod := MakeReservationPod(pod)

	if err := schedulerCache.AddPod(rpod); err != nil {
		klog.V(4).Infof("ReservationPod %s/%s AddPod fail %s", pod.Namespace, pod.Name, err)
	} else {
		klog.V(4).Infof("ReservationPod %s/%s AddPod success", pod.Namespace, pod.Name)
	}
}

//CleanReservationPodForSchedule clean reservation pod for schedule
func CleanReservationPodForSchedule(pod *v1.Pod, pVCLister corelisters.PersistentVolumeClaimLister, schedulerCache schedulerinternalcache.Cache) ([]*v1.Pod, bool) {
	var reservationPods []*v1.Pod
	if IsReservationPod(pod, pVCLister) {
		reservationPods = checkPVCAnnotationAndReservation(pod, pVCLister, schedulerCache)
		rpod := MakeReservationPod(pod)
		cachePod, err := schedulerCache.GetPod(rpod)
		if err != nil {
			klog.V(4).Infof("CleanReservationPod %s/%s GetPod fail error %s", rpod.Namespace, rpod.Name, err)
			//reservationPods = checkPVCAnnotationAndReservation(pod, pVCLister, schedulerCache)
		} else {
			err = schedulerCache.RemovePod(cachePod)
			if err != nil {
				klog.V(4).Infof("CleanReservationPod %s/%s RemovePod fail error %s", cachePod.Namespace, cachePod.Name, err)
			}
			klog.V(4).Infof("CleanReservationPod %s/%s RemovePod success", cachePod.Namespace, cachePod.Name)
			reservationPods = append(reservationPods, cachePod)
		}
		//reservationPods = append(reservationPods, checkPVCAnnotationAndReservation(pod, pVCLister, schedulerCache))
	}
	if len(reservationPods) > 0 {
		return reservationPods, true
	}
	return reservationPods, false
}

//checkPVCAnnotationAndReservation clean importer pod when pvc bound to vm pod
func checkPVCAnnotationAndReservation(pod *v1.Pod, pVCLister corelisters.PersistentVolumeClaimLister, schedulerCache schedulerinternalcache.Cache) (reservationPods []*v1.Pod) {
	for _, volume := range pod.Spec.Volumes {
		if volume.PersistentVolumeClaim != nil {
			if volume.PersistentVolumeClaim.ClaimName != "" {
				pvcName := volume.PersistentVolumeClaim.ClaimName
				klog.V(4).Infof(" %s/%s pv %s", pod.Namespace, pod.Name, pvcName)
				pvc, err := pVCLister.PersistentVolumeClaims(pod.Namespace).Get(pvcName)
				if err != nil {
					klog.V(4).Infof("CleanReservationPod %s/%s pv %s error %s", pod.Namespace, pod.Name, pvcName, err)
					continue
				}
				if pvc.Annotations != nil {
					ppod := &v1.Pod{}
					err := json.Unmarshal([]byte(pvc.Annotations[AnnoBoundPod]), ppod)
					if err != nil {
						klog.V(4).Infof("CleanReservationPod %s/%s Unmarshal annotation %s fail %s", pod.Namespace, pod.Name, pvc.Annotations[AnnoBoundPod], err)
						continue
					}
					rpod := MakeReservationPod(ppod)
					cachePod, err := schedulerCache.GetPod(rpod)
					if err == nil {
						err = schedulerCache.RemovePod(cachePod)
						if err != nil {
							klog.V(4).Infof("CleanReservationPod %s/%s RemovePod fail error %s", cachePod.Namespace, cachePod.Name, err)
						}
						klog.V(4).Infof("CleanReservationPod %s/%s RemovePod success", cachePod.Namespace, cachePod.Name)
						reservationPods = append(reservationPods, cachePod)
					}
				}
			}
		}
	}
	return
}

//AddReservationPodBack add reservation pod back when schedule fail
func AddReservationPodBack(pods []*v1.Pod, pVCLister corelisters.PersistentVolumeClaimLister, schedulerCache schedulerinternalcache.Cache) {
	for _, pod := range pods {
		if err := schedulerCache.AddPod(pod); err != nil {
			klog.V(4).Infof("AddReservationPodBack %s/%s AddPod fail %s", pod.Namespace, pod.Name, err)
		} else {
			klog.V(4).Infof("AddReservationPodBack %s/%s AddPod success", pod.Namespace, pod.Name)
		}
	}
}
