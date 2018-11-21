package types

import (
	"aita_chain/common"
	"aita_chain/crypto"
	"aita_chain/gVar"
	"aita_chain/utils"
	"crypto/ecdsa"
	"crypto/sha256"
	"fmt"
)

type TxDecSet struct {
	SenderInShardID uint32
	Round           uint32
	TxlistHashID    common.Hash
	MemCnt          uint32 // TODO ?
	ShardIndexFrom  uint32
	MemDecisions    []TransactionListDecision
	TxCnt           uint32
	TxArray         []common.Hash
	Sig AitaSign
}

//txDecRev request sync
type TxDecSetRev struct {
	ShardID uint32
	Round   uint32
}

//Add adds a TxDecision
func (tds *TxDecSet) AddDecision(b *TransactionListDecision) {
	tds.MemCnt++
	tds.MemDecisions = append(tds.MemDecisions, *b)
}

func (tds *TxDecSet) Sign(prk *ecdsa.PrivateKey) {
	tmp := make([]byte, 0, 36+int(tds.TxCnt/4*tds.MemCnt))
	tmp = append(utils.ByteSlice(tds.SenderInShardID), tds.TxlistHashID[:]...)
	for i := uint32(0); i < tds.MemCnt; i++ {
		tmp = append(tmp, tds.MemDecisions[i].Decision...)
	}
	tmpHash := sha256.Sum256(tmp)

	if sig, err := crypto.Sign(tmpHash[:], prk); err == nil {
		tds.Sig = sig
	} else {
		fmt.Println("Sign Error !!!")
	}
}

func (tds *TxDecSet) VerifySig(puk []byte) bool {
	tmp := make([]byte, 0, 36+int(tds.TxCnt/4*tds.MemCnt))
	tmp = append(utils.ByteSlice(tds.SenderInShardID), tds.TxlistHashID[:]...)
	for i := uint32(0); i < tds.MemCnt; i++ {
		tmp = append(tmp, tds.MemDecisions[i].Decision...)
	}
	tmpHash := sha256.Sum256(tmp)

	passed := crypto.VerifySignature(puk, tmpHash[:], tds.Sig)
	return passed
}

//Set init an instance of TxDecSet given those parameters
func (tds *TxDecSet) Set(txl *TxList, shardIdx uint32, isSelfShard bool) {
	tds.SenderInShardID = txl.SenderInShardID
	tds.TxlistHashID = txl.HashID
	tds.MemCnt = 0
	tds.Round = txl.Round
	tds.ShardIndexFrom = shardIdx
	if isSelfShard == true {
		tds.TxCnt = 0
		tds.TxArray = nil
	} else {
		// this will work if there is no replies, what a trick
		tds.TxCnt = txl.TxCnt
		tds.TxArray = make([]common.Hash, 0, tds.TxCnt)
		//a.TxArrayX = make([][SHash]byte, 0, a.TxCnt)
		for i := uint32(0); i < tds.TxCnt; i++ {
			tds.TxArray = append(tds.TxArray, tds.TxArray[i])
		}
	}
}

//Result is the result of the index-th transaction
//passed if half of shard members say yes
func (a *TxDecSet) Result(index uint32) bool {
	x := index / 8
	y := byte(index % 8)
	var ans uint32
	for i := uint32(0); i < a.MemCnt; i++ {
		ans = ans + uint32((a.MemDecisions[i].Decision[x]>>y)&1)
	}
	if ans > (gVar.ShardSize-1)/2 {
		return true
	}
	return true
}
