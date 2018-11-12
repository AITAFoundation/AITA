package shard

import (
	"fmt"

	"aita_chain/account"
	"aita_chain/ed25519"
	"aita_chain/gVar"
)

//MemShard is the struct of miners for sharding and leader selection
type MemShard struct {
	//TCPAddress  *net.TCPAddr
	Address        string //ip+port
	PrivateAddress string
	PublicAddress  string
	Rep            int64   //rep this epoch
	TotalRep       []int64 //rep over several epoch
	CosiPub        ed25519.PublicKey
	Shard          int
	InShardId      int
	Role           byte //1 - member, 0 - leader
	Legal          byte //0 - legal,  1 - kickout
	RealAccount    *account.RcAcc
	PreShard       int
	Bandwidth      int
}

//NewMemShard new a mem shard, addr - ip + port
func (ms *MemShard) NewMemShard(acc *account.RcAcc, addr string, band int) {
	ms.Address = addr
	ms.PrivateAddress = addr
	//ms.TCPAddress,_ = net.ResolveTCPAddr("tcp", addr)
	ms.RealAccount = acc
	ms.CosiPub = acc.CosiPuk
	ms.Legal = 0
	ms.Role = 1
	ms.Rep = 0
	ms.Bandwidth = band
}

//NewTotalRep set a new total rep to 0
func (ms *MemShard) NewTotalRep() {
	ms.TotalRep = []int64{}
}

//CopyTotalRepFromSB copy total rep from sync bock
func (ms *MemShard) CopyTotalRepFromSB(value []int64) {
	ms.TotalRep = make([]int64, len(value))
	copy(ms.TotalRep, value)
}

//ClearTotalRep is clear total rep
func (ms *MemShard) ClearTotalRep() {
	for i := 0; i < len(ms.TotalRep); i++ {
		ms.TotalRep[i] = 0
	}
}

//SetTotalRep set totalrep
func (ms *MemShard) SetTotalRep(value int64) {
	if len(ms.TotalRep) == gVar.SlidingWindows {
		ms.TotalRep = ms.TotalRep[1:]
	}
	ms.TotalRep = append(ms.TotalRep, value)
}

//AddRep add a reputation value
func (ms *MemShard) AddRep(value int64) {
	ms.Rep += value
}

//CalTotalRep cal total rep over epoches
func (ms *MemShard) CalTotalRep() int64 {
	sum := int64(0)
	for i := range ms.TotalRep {
		sum += ms.TotalRep[i]
	}
	return sum
}

//ClearRep clear rep
func (ms *MemShard) ClearRep() {
	ms.Rep = 0
}

//Print prints the sharding information
func (ms *MemShard) Print() {
	fmt.Println()
	fmt.Println("Member data:")
	fmt.Println("Addres:", ms.Address)
	fmt.Println("Rep:", ms.Rep)
	fmt.Println("TotalRep:", ms.TotalRep)
	fmt.Println("Shard:", ms.Shard)
	fmt.Println("InShardId:", ms.InShardId)
	if ms.Role == 0 {
		fmt.Println("Role:Leader")
	} else {
		fmt.Println("Role:Member")
	}

}
