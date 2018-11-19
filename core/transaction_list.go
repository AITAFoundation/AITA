package types

import (
	"aita_chain/common"
	"aita_chain/crypto"
	"aita_chain/utils"
	"crypto/ecdsa"
	"crypto/sha256"
	"fmt"
)

type TxList struct {
	SenderInShardID uint32
	//ID      uint32
	HashID  [32]byte
	Round   uint32
	TxCnt   uint32
	TxArray []common.Hash
	sig     AitaSign
	//TODO signature
}

type StatusCheck struct {
	UnknownTxNum int
	Valid []int
	Channel chan bool
}

//Hash returns the ID of the TxList
func (txl *TxList) Hash() common.Hash {
	tmp := make([]byte, 0, 4+txl.TxCnt*32)
	tmp = utils.ByteSlice(txl.Round)
	for i := uint32(0); i < txl.TxCnt; i++ {
		tmp = append(tmp, txl.TxArray[i][:]...)
	}
	return sha256.Sum256(tmp)
}


//AddTx adds the tx into transaction list
func (txl *TxList) AddTx(tx *Transaction) {
	txl.TxCnt++
	txl.TxArray = append(txl.TxArray, tx.GetHash())
	//txl.TxArrayX = append(a.TxArrayX, HashCut(tx.Hash))
}

func (tl *TxList) WithSignature(sig []byte) {
	tl.sig = sig
}

func (tl *TxList) VerifyTxList(puk []byte) error {
	hash := tl.HashWithRLP()

	passed := crypto.VerifySignature(puk, hash[:], tl.sig)
	if passed == true {
		return nil
	} else {
		return fmt.Errorf("cannot verify txlist : %v", hash)
	}
}

func (tl *TxList) SignECDSA(pri *ecdsa.PrivateKey) ([]byte, error) {
	tl.HashID = tl.HashWithRLP()
	sig, err := crypto.Sign(tl.HashID[:], pri)
	return sig, err
}

func (tl *TxList) Sign(pri *ecdsa.PrivateKey) {
	//FIXME no error check
	sig, _ := tl.SignECDSA(pri)
	tl.sig = sig
}

func (tl *TxList) HashWithRLP() common.Hash {
	return RlpHash([]interface{}{
		tl.SenderInShardID,
		tl.TxCnt,
		tl.TxArray,
		tl.Round,
	})
}
