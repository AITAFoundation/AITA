// Copyright 2015 The go-aita Authors
// This file is part of the go-aita library.
//
// The go-aita library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-aita library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-aita library. If not, see <http://www.gnu.org/licenses/>.
package aita

import (
	"aita_chain/common"
	"aita_chain/core/shard"
	"aita_chain/gVar"
	"aita_chain/params"
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"aita_chain/aita/downloader"
	"aita_chain/aita/fetcher"
	"aita_chain/aitadb"
	"aita_chain/consensus"
	"aita_chain/core"
	"aita_chain/core/types"
	"aita_chain/event"
	"aita_chain/log"
	"aita_chain/p2p"
	"aita_chain/p2p/anode"
)

const (
	txChanSize = 4096

	leader    = 0
	member    = 1
	xcrossed  = 1
	sameshard = 0
	yes       = 1
	no        = 0

	// leader decision time out (unit: s)
	leaderDecisionTimeOut = 3600

	// enough number to start a service
	EpochReadyNum = 0
)

var (
	daoChallengeTimeout = 15 * time.Second // Time allowance for a node to reply to the DAO handshake challenge
)

// errIncompatibleConfig is returned if the requested protocols and configs are
// not compatible (low protocol version restrictions and high requirements).
var errIncompatibleConfig = errors.New("incompatible configuration")

func errResp(code errCode, format string, v ...interface{}) error {
	return fmt.Errorf("%v - %v", code, fmt.Sprintf(format, v...))
}

type ProtocolManager struct {
	networkID uint64

	fastSync  uint32 // Flag whether fast sync is enabled (gets disabled if we already have blocks)
	acceptTxs uint32 // Flag whether we're considered synchronised (enables transaction processing)

	txpool txPool

	maxPeers int

	downloader *downloader.Downloader
	fetcher    *fetcher.Fetcher
	peers      *peerSet
	// NOTE: peer pool is used to maintain all connected peers
	peerPool PeerPool

	// USE CoreCache
	coreCache *shard.CoreCache

	SubProtocols []p2p.Protocol

	eventMux      *event.TypeMux
	txsCh         chan core.NewTxsEvent
	txsSub        event.Subscription
	minedBlockSub *event.TypeMuxSubscription

	// channels for fetcher, syncer, txsyncLoop

	newPeerCh   chan *peer
	txsyncCh    chan *txsync
	quitSync    chan struct{}
	noMorePeers chan struct{}

	wg sync.WaitGroup
}

func NewProtocolManager(config *params.ChainConfig, mode downloader.SyncMode, networkID uint64, mux *event.TypeMux, txpool txPool, engine consensus.Engine, blockchain interface{}, chaindb aitadb.Database, coreCache *shard.CoreCache) (*ProtocolManager, error) {
	// Create the protocol manager with the base fields
	manager := &ProtocolManager{
		networkID:   networkID,
		eventMux:    mux,
		txpool:      txpool,
		peers:       newPeerSet(),
		newPeerCh:   make(chan *peer),
		noMorePeers: make(chan struct{}),
		txsyncCh:    make(chan *txsync),
		quitSync:    make(chan struct{}),

		//goShard:     make(chan int),
		coreCache: coreCache,
		peerPool:  GetPeerPoolIns(),
	}

	// Initiate a sub-protocol for every implemented version we can handle
	manager.SubProtocols = make([]p2p.Protocol, 0, len(ProtocolVersions))
	for i, version := range ProtocolVersions {

		version := version // Closure for the run
		manager.SubProtocols = append(manager.SubProtocols, p2p.Protocol{
			Name:    ProtocolName,
			Version: version,
			Length:  ProtocolLengths[i],
			Run: func(p *p2p.Peer, rw p2p.MsgReadWriter) error {
				peer := manager.newPeer(int(version), p, rw)
				select {
				case manager.newPeerCh <- peer:
					manager.wg.Add(1)
					defer manager.wg.Done()
					return manager.handle(peer)
				case <-manager.quitSync:
					return p2p.DiscQuitting
				}
			},

			PeerInfo: func(id anode.ID) interface{} {
				if p := manager.peers.Peer(fmt.Sprintf("%x", id[:8])); p != nil {
					return p.Info()
				}
				return nil
			},
		})
	}
	if len(manager.SubProtocols) == 0 {
		return nil, errIncompatibleConfig
	}

	manager.peerPool.InitProtocol(manager.SubProtocols)

	return manager, nil
}

func (pm *ProtocolManager) removePeer(id string) {
	// Short circuit if the peer was already removed
	peer := pm.peers.Peer(id)
	if peer == nil {
		return
	}
	log.Debug("Removing Aita peer", "peer", id)

	if err := pm.peers.Unregister(id); err != nil {
		log.Error("Peer removal failed", "peer", id, "err", err)
	}
	// Hard disconnect at the networking layer
	if peer != nil {
		peer.Peer.Disconnect(p2p.DiscUselessPeer)
	}
}

func (pm *ProtocolManager) Start(maxPeers int) {
	pm.maxPeers = maxPeers

	// broadcast transactions
	pm.txsCh = make(chan core.NewTxsEvent, txChanSize)
	pm.txsSub = pm.txpool.SubscribeNewTxsEvent(pm.txsCh)
	node, exist := pm.peerPool.PM().GetSelf()
	if exist {
		pm.coreCache.ShardID = node.ShardId
		pm.coreCache.InShardID = node.InShardId
		pm.coreCache.Role = int(node.Type)
	}

	go pm.CheckReady()
	<-pm.coreCache.GoShard
	go pm.txBroadcastLoop()

	go func(pm *ProtocolManager) {
		var i = 0
		for {
			for {
				if len(pm.coreCache.TxListGroupPool) == 0 {
					time.Sleep(time.Second * 1)
				} else {
					break
				}
			}
			go pm.txListProcess()
			time.Sleep(time.Second * 1)
			i++
		}

	}(pm)

}

func (pm *ProtocolManager) CheckReady() {
	for {
		time.Sleep(time.Microsecond * 3000)
		// FIXME LEN(PEERS) IS 1, WHEN NO PEERS
		length := pm.peerPool.PM().GetPeerCount(pm.coreCache.ShardID)
		if length >= EpochReadyNum {
			pm.coreCache.GoShard <- true
			break
		}
		time.Sleep(time.Microsecond * 3000)
	}

}

func (pm *ProtocolManager) txListProcess() {
	TLG := <-pm.coreCache.TxListGroupPool
	pm.signTxList(TLG)
	go pm.sendTxList()
	timeoutflag := true
	cnt := 1
	thisRoundIdx := TLG.TLS[pm.coreCache.ShardID].Round
	ch := pm.coreCache.TxlDeChanMap.Get(thisRoundIdx)
	for timeoutflag && cnt < int(gVar.ShardSize) {
		select {
		case <-ch:
			cnt++
		case <-time.After(leaderDecisionTimeOut * time.Second):
			fmt.Println("TxDecSet is not full", cnt, "in total")
			timeoutflag = false
		}
	}
	fmt.Println("TxDec of Round", thisRoundIdx, "total txdec: ", uint32(cnt))

	// NOTE use a global var to track the round
	if thisRoundIdx-pm.coreCache.PrevHeight > 0 {
		for {
			pm.coreCache.TxlProcessedNumLock.Lock()
			v := pm.coreCache.TxlProcessedNum
			pm.coreCache.TxlProcessedNumLock.Unlock()
			if v == thisRoundIdx-pm.coreCache.PrevHeight-1 {
				break
			}
			time.Sleep(time.Microsecond * 500)
		}
	}

	pm.signTxDecSet(TLG)
	pm.processTransactionDecisionSet(TLG.TDS[pm.coreCache.ShardID])

	go pm.sendTxDecSet(TLG)
	go pm.NormalTxBlock(thisRoundIdx - pm.coreCache.PrevHeight)

	pm.coreCache.TxlProcessedNumLock.Lock()
	pm.coreCache.TxlProcessedNum++
	pm.coreCache.TxlProcessedNumLock.Unlock()
	pm.coreCache.TxListGoroutineTokens <- true
}

func (pm *ProtocolManager) release(tlg *shard.TLGroup) {
	pm.coreCache.TxlDeChanMap.Delete(tlg.TLS[pm.coreCache.ShardID].Round)
	hash := tlg.TLS[pm.coreCache.ShardID].HashID
	delete(pm.coreCache.TlgMap, hash)
}

// Gen and send TxBlock
func (pm *ProtocolManager) NormalTxBlock(currentRound uint32) {
	if currentRound > 0 {
		//<-pm.coreCache.TBBChan[pm.coreCache.CurrentRound-1]
		for {
			pm.coreCache.TxblkSequenceLock.Lock()
			v := pm.coreCache.TxBlkSequence
			pm.coreCache.TxblkSequenceLock.Unlock()
			if v == currentRound-1 {
				break
			}
			time.Sleep(time.Microsecond * 500)
		}
	}
	pm.GenTxBlock()
	go pm.SendTxBlock()

	pm.coreCache.TxblkSequenceLock.Lock()
	pm.coreCache.TxBlkSequence++
	pm.coreCache.TxblkSequenceLock.Unlock()
}

func (pm *ProtocolManager) TxLastBlock() {
	lastFlag := true
	for lastFlag {
		tmpEpoch := <-pm.coreCache.StartLastTxBlock
		if tmpEpoch == pm.coreCache.CurrentEpoch {
			lastFlag = false
		}
	}
}

// generate transaction block
func (pm *ProtocolManager) GenTxBlock() {
	height := pm.coreCache.TxBlkChain.Height

	pm.coreCache.TxBlock = new(core.TxBlock)
	pm.coreCache.TxBlock.MakeTxBlock(pm.coreCache.InShardID, &pm.coreCache.Ready, pm.coreCache.TxBlkChain.LastTB, pm.coreCache.Prikey, height+1, 0, nil, 0)

	pm.coreCache.Ready = nil
	*(pm.coreCache.TBCache) = append(*(pm.coreCache.TBCache), pm.coreCache.TxBlock.HashID)
	pm.coreCache.TxCntProcessed += pm.coreCache.TxBlock.TxCnt
	pm.coreCache.TxBlkChain.AddBlock(pm.coreCache.TxBlock)
}

// send transaction block
func (pm *ProtocolManager) SendTxBlock() {
	peers := pm.peerPool.PM().GetPeers(pm.coreCache.ShardID)
	for _, p := range peers {
		peer, ok := convert(p)
		if ok && peer.InShardID != pm.coreCache.InShardID {
			go peer.SendTxBlock(pm.coreCache.TxBlock)
		}
	}
}

func (pm *ProtocolManager) PreprocessTxBlock(block *core.TxBlock, sc *types.StatusCheck) error {
	if sc == nil {
		return fmt.Errorf("statuscheck is nil")
	}
	if sc.UnknownTxNum == gVar.INITTIAL_STATUS {
		if int(block.TxCnt) != len(block.TxArray) {
			return fmt.Errorf("PreTxBlock: TxBlock parameter not match")
		}
		sc.UnknownTxNum = int(block.TxCnt)
		sc.Valid = make([]int, block.TxCnt)
		for i := uint32(0); i < block.TxCnt; i++ {
			_, ok := pm.coreCache.TxHashCache[block.TxHash[i]]
			if !ok {
				tmpWait, okW := pm.coreCache.WaitHashCache[block.TxHash[i]]
				if okW {

					tmpWait.DataTB = append(tmpWait.DataTB, block)

					tmpWait.StatTB = append(tmpWait.StatTB, sc)
					// NOTE i-th transaction
					tmpWait.IDTB = append(tmpWait.IDTB, int(i))
				} else {
					tmpWait = shard.WaitProcess{nil, nil, nil, nil, nil, nil, nil, nil, nil}
					tmpWait.DataTB = append(tmpWait.DataTB, block)
					tmpWait.StatTB = append(tmpWait.StatTB, sc)
					tmpWait.IDTB = append(tmpWait.IDTB, int(i))
				}
				pm.coreCache.WaitHashCache[block.TxHash[i]] = tmpWait
			} else {
				sc.UnknownTxNum--
				sc.Valid[i] = gVar.TX_KNOWN
			}
		}
	}

	if sc.UnknownTxNum > 0 {
		sc.Channel = make(chan bool, sc.UnknownTxNum)
	}
	return nil
}

//GetTxBlock handle the txblock sent by the leader
func (pm *ProtocolManager) GetTxBlock(a *core.TxBlock) error {
	pm.coreCache.TXBlockRWL.Lock()
	if pm.coreCache.TxBlock != nil {
		if a.Height != pm.coreCache.TxBlock.Height+1 {
			return fmt.Errorf("Height not match")
		}
		for i := uint32(0); i < a.TxCnt; i++ {
			pm.coreCache.ClearCache(a.TxHash[i])
		}
		*(pm.coreCache.TBCache) = append(*(pm.coreCache.TBCache), a.HashID)
		pm.coreCache.TxBlock = a
		pm.coreCache.TxCntProcessed += pm.coreCache.TxBlock.TxCnt
		pm.coreCache.TxBlkChain.AddBlock(a)
		pm.coreCache.CurrentRound++
	}

	pm.coreCache.TXBlockRWL.Unlock()

	return nil
}

func (pm *ProtocolManager) HandleTxBlock(txblk *core.TxBlock) error {
	sc := types.StatusCheck{UnknownTxNum: gVar.INITTIAL_STATUS, Valid: nil}
	pm.PreprocessTxBlock(txblk, &sc)
	timeoutFlag := true
	cnt := sc.UnknownTxNum
	for timeoutFlag && sc.UnknownTxNum > 0 {
		select {
		case <-sc.Channel:
			cnt--
		case <-time.After(60 * time.Second):
			timeoutFlag = false
		}
	}

	if pm.coreCache.CurrentRound > 0 {
		//<-pm.coreCache.TBBChan[pm.coreCache.CurrentRound-1]
		for {
			pm.coreCache.TxblkSequenceLock.Lock()
			v := pm.coreCache.TxBlkSequence
			pm.coreCache.TxblkSequenceLock.Unlock()
			if v == pm.coreCache.CurrentRound-1 {
				break
			}
			time.Sleep(time.Microsecond * 500)
		}
	}

	flag := true
	//fmt.Println("TxB Kind", tmp.Kind)

	for flag {
		err := pm.GetTxBlock(txblk)
		if err != nil {
			//fmt.Println("txBlock", base58.Encode(tmp.HashID[:]), " error", err)
		} else {
			flag = false
		}
		time.Sleep(time.Microsecond * gVar.GeneralSleepTime)
	}

	pm.coreCache.TxblkSequenceLock.Lock()
	pm.coreCache.TxBlkSequence++
	pm.coreCache.TxblkSequenceLock.Unlock()

	//if txblk.Height <= pm.coreCache.PrevHeight+gVar.NumTxListPerEpoch {
	//	pm.coreCache.TBChan[txblk.Height-pm.coreCache.PrevHeight-1] <- pm.coreCache.CurrentEpoch
	//}
	// NOTE don't check bad blocks
	//if tmp.Kind != 3 {
	//
	//} else {
	//	fmt.Println(time.Now(), CacheDbRef.ID, "gets a bad txBlock with", tmp.TxCnt, "Txs from", tmp.ID, "Hash", base58.Encode(tmp.HashID[:]), "Height:", tmp.Height)
	//	RollingProcess(true, false, tmp)
	//}
	return nil
}

func (pm *ProtocolManager) signTxDecSet(tlg *shard.TLGroup) {
	for i := uint32(0); i < gVar.ShardCnt; i++ {
		tlg.TDS[i].Sign(pm.coreCache.Prikey)
	}
}

func (pm *ProtocolManager) sendTxDecSet(tlg *shard.TLGroup) {

	go pm.sendTxDecSetInShard(tlg)
	//NOTE RANDOM SEED
	rand.Seed(time.Now().Unix() + rand.Int63())
	for i := uint32(0); i < gVar.ShardCnt; i++ {
		p := pm.peerPool.PM().GetRandom(i)
		peer, ok := convert(p)
		if ok {
			go peer.SendXShardTxDecSet(tlg.TDS[peer.ShardID])
		}
	}

	cnt := 1
	mask := make([]bool, gVar.ShardCnt)
	mask[pm.coreCache.ShardID] = true
	for cnt < int(gVar.ShardCnt) {
		select {
		case tmp := <-pm.coreCache.TxDecRevChan[pm.coreCache.CurrentRound]:
			fmt.Println("Get txdecRev from", tmp.ShardID)
			mask[tmp.ShardID] = true
			cnt++
		case <-time.After(5 * time.Second):
			for i := uint32(0); i < gVar.ShardCnt; i++ {
				if !mask[i] {
					p := pm.peerPool.PM().GetRandom(i)
					peer, ok := convert(p)
					if ok {
						peer.SendXShardTxDecSet(tlg.TDS[peer.ShardID])
					}
				}
			}
		}
	}
	pm.release(tlg)
}

func (pm *ProtocolManager) PreprocessTxDecSet(tds *types.TxDecSet, sc *types.StatusCheck) error {
	if sc == nil {
		return fmt.Errorf("statuscheck is nil")
	}
	if sc.UnknownTxNum == gVar.INITTIAL_STATUS {
		sc.UnknownTxNum = int(tds.TxCnt)
		sc.Valid = make([]int, tds.TxCnt)
		for i := uint32(0); i < tds.TxCnt; i++ {
			_, ok := pm.coreCache.TxHashCache[tds.TxArray[i]]
			if !ok {
				tmpWait, okW := pm.coreCache.WaitHashCache[tds.TxArray[i]]
				if okW {
					tmpWait.DataTDS = append(tmpWait.DataTDS, tds)
					tmpWait.StatTDS = append(tmpWait.StatTDS, sc)
					tmpWait.IDTDS = append(tmpWait.IDTDS, int(i))
				} else {
					tmpWait = shard.WaitProcess{nil, nil, nil, nil, nil, nil, nil, nil, nil}
					tmpWait.DataTDS = append(tmpWait.DataTDS, tds)
					tmpWait.StatTDS = append(tmpWait.StatTDS, sc)
					tmpWait.IDTDS = append(tmpWait.IDTDS, int(i))
				}
				pm.coreCache.WaitHashCache[tds.TxArray[i]] = tmpWait
			} else {
				sc.UnknownTxNum--
				sc.Valid[i] = gVar.TX_KNOWN
			}
		}
	}
	if sc.UnknownTxNum > 0 {
		sc.Channel = make(chan bool, sc.UnknownTxNum)
	}
	return nil
}

func (pm *ProtocolManager) HandleTxDecSetLeader(tds *types.TxDecSet) error {
	statusCheck := types.StatusCheck{UnknownTxNum: gVar.INITTIAL_STATUS, Valid: nil}
	pm.PreprocessTxDecSet(tds, &statusCheck)
	flag := true
	if statusCheck.UnknownTxNum == 0 {
		flag = false
	}
	for flag {
		time.Sleep(time.Microsecond * 50)
		if statusCheck.UnknownTxNum == 0 {
			flag = false
		}
	}
	flag = false
	pm.processTransactionDecisionSet(tds)
	pm.coreCache.TDSCnt[tds.ShardIndexFrom]++
	if pm.coreCache.TDSCnt[tds.ShardIndexFrom] == gVar.NumTxListPerEpoch {
		pm.coreCache.TDSNotReady--
		if pm.coreCache.TDSNotReady == 0 {
			flag = true
		}
	}

	if flag {
		pm.coreCache.StartLastTxBlock <- pm.coreCache.CurrentEpoch
	}
	return nil
}

func (pm *ProtocolManager) HandleTxDecRecv(tdsr *types.TxDecSetRev) error {
	pm.coreCache.TxDecRevChan[tdsr.Round-pm.coreCache.PrevHeight] <- *tdsr
	return nil
}

func (pm *ProtocolManager) HandleTxDecSet(tds *types.TxDecSet, inputType int) error {
	if tds.Round < pm.coreCache.PrevHeight {
		return fmt.Errorf("Previous epoch packet")
	}
	if inputType == xcrossed {
		var tdr types.TxDecSetRev
		tdr.ShardID = pm.coreCache.ShardID
		tdr.Round = tds.Round
		p := pm.peerPool.PM().Get(tds.ShardIndexFrom, tds.SenderInShardID)
		peer, ok := p.(*peer)

		if ok && peer != nil {
			go peer.SendTxDecSetRecv(&tdr)
		}
	}
	return nil
}

func (pm *ProtocolManager) HandleAndSendTxDecSet(tds *types.TxDecSet) error {
	err := pm.HandleTxDecSet(tds, xcrossed)
	if err != nil {
		return err
	}

	peers := pm.peerPool.PM().GetPeers(pm.coreCache.ShardID)
	for _, prIf := range peers {
		pr := prIf.(*peer)
		if pr.InShardID != pm.coreCache.InShardID {
			go pr.SendInShardTxDecSet(tds)
		}
	}
	return nil
}

func (pm *ProtocolManager) sendTxDecSetInShard(tlg *shard.TLGroup) {

	ps := pm.peerPool.PM().GetPeers(pm.coreCache.ShardID)
	if len(ps) > 0 {
		fmt.Println("U go peers ++++++")
		for _, p := range ps {
			peer, ok := convert(p)
			if ok && peer.InShardID != pm.coreCache.InShardID {
				go peer.SendInShardTxDecSet(tlg.TDS[pm.coreCache.ShardID])
			}
		}
	} else {
		fmt.Println("U cannot go peers ++++++")
	}

}

func (pm *ProtocolManager) processTransactionDecisionSet(tds *types.TxDecSet) {
	if tds.ShardIndexFrom == pm.coreCache.ShardID {
		tmpTlg, ok := pm.coreCache.TlgMap[tds.TxlistHashID]
		if ok {
			tds.TxCnt = tmpTlg.TLS[pm.coreCache.ShardID].TxCnt
			tds.TxArray = tmpTlg.TLS[pm.coreCache.ShardID].TxArray
		}
	}

	for i := uint32(0); i < tds.TxCnt; i++ {
		tmpTxHash := tds.TxArray[i]
		tmpXShardDec := pm.coreCache.CacheXshardDecs.Get(tmpTxHash)
		if tmpXShardDec == nil {
			tmpXShardDec = new(types.CrossShardDec)
			tmpXShardDec.NewFromOther(tds.ShardIndexFrom, tds.Result(i))
			pm.coreCache.CacheXshardDecs.Set(tmpTxHash, tmpXShardDec)
		} else {
			tmpXShardDec.UpdateFromOther(tds.ShardIndexFrom, tds.Result(i))
			if tmpXShardDec.Res == gVar.RESULT_YES {
				pm.coreCache.Ready = append(pm.coreCache.Ready, *(tmpXShardDec.Data))
			}
			if tmpXShardDec.Total == 0 {
				pm.coreCache.CacheXshardDecs.Delete(tmpTxHash)
			} else {
				pm.coreCache.CacheXshardDecs.Set(tmpTxHash, tmpXShardDec)
			}
		}
	}

	if tds.ShardIndexFrom == pm.coreCache.ShardID {
		tds.TxCnt = 0
		tds.TxArray = nil
	}
}

func (pm *ProtocolManager) signTxList(tlg *shard.TLGroup) {

	selfShardID := pm.coreCache.ShardID
	tlg.TLS[selfShardID].Sign(pm.coreCache.Prikey)
	pm.coreCache.TxCnt += tlg.TLS[selfShardID].TxCnt

	for i := uint32(0); i < gVar.ShardCnt; i++ {
		if selfShardID != i {
			tlg.TLS[i].HashID = tlg.TLS[i].Hash()
		}
	}
	for i := uint32(0); i < gVar.ShardCnt; i++ {
		if i == selfShardID {
			tlg.TDS[i].Set(tlg.TLS[i], selfShardID, true)
		} else {
			tlg.TDS[i].Set(tlg.TLS[i], selfShardID, false)
		}
	}
	pm.coreCache.TlgMap[tlg.TLS[pm.coreCache.ShardID].HashID] = tlg
}

func (pm *ProtocolManager) sendTxList() {
	for {
		v := <-pm.coreCache.TxlReadyPool
		pm.sendTxListToShardMembers(v)
	}
}

func (pm *ProtocolManager) sendTxListToShardMembers(tlg *shard.TLGroup) {
	for _, p := range pm.peerPool.PM().GetPeers(pm.coreCache.ShardID) {
		peer, ok := convert(p)
		if ok {
			go peer.SendTransactionList(tlg.TLS[pm.coreCache.ShardID])
		}
	}
}

func (pm *ProtocolManager) Stop() {
	log.Info("Stopping Aita protocol")

	pm.txsSub.Unsubscribe()
	pm.minedBlockSub.Unsubscribe()

	pm.noMorePeers <- struct{}{}

	close(pm.quitSync)

	pm.peers.Close()

	pm.wg.Wait()

	log.Info("Aita protocol stopped")
}

func (pm *ProtocolManager) newPeer(pv int, p *p2p.Peer, rw p2p.MsgReadWriter) *peer {
	return newPeer(pv, p, newMeteredMsgWriter(rw))
}

func (pm *ProtocolManager) handle(p *peer) error {
	if pm.peers.Len() >= pm.maxPeers && !p.Peer.Info().Network.Trusted {
		return p2p.DiscTooManyPeers
	}
	p.Log().Debug("Aita peer connected", "name", p.Name())

	var (
		td = new(big.Int)
	)
	var (
		p1 common.Hash
		p2 common.Hash
	)
	if err := p.Handshake(pm.networkID, td, p1, p2); err != nil {
		p.Log().Debug("Aita handshake failed", "err", err)
		return err
	}

	if rw, ok := p.rw.(*meteredMsgReadWriter); ok {
		rw.Init(p.version)
	}
	// Register the peer locally
	if err := pm.peers.Register(p); err != nil {
		p.Log().Error("Aita peer registration failed", "err", err)
		return err
	}
	defer pm.removePeer(p.id)

	// Register the peer in the downloader. If the downloader considers it banned, we disconnect
	if err := pm.downloader.RegisterPeer(p.id, p.version, p); err != nil {
		return err
	}
	pm.syncTransactions(p)

	for {
		if err := pm.handleMsg(p); err != nil {
			p.Log().Debug("Aita message handling failed", "err", err)
			return err
		}
	}
}

func (pm *ProtocolManager) handleMsg(p *peer) error {
	msg, err := p.rw.ReadMsg()
	if err != nil {
		return err
	}
	if msg.Size > ProtocolMaxMsgSize {
		return errResp(ErrMsgTooLarge, "%v > %v", msg.Size, ProtocolMaxMsgSize)
	}
	defer msg.Discard()

	// Handle the message depending on its contents
	switch {
	case msg.Code == StatusMsg:
		// Status messages should never arrive after the handshake
		return errResp(ErrExtraStatusMsg, "uncontrolled status message")
	case msg.Code == TxMsg:
		// Transactions arrived, make sure we have a valid and fresh chain to handle them
		if atomic.LoadUint32(&pm.acceptTxs) == 0 {
			break
		}
		// Transactions can be processed, parse all of them and deliver to the pool
		var txs []*types.Transaction
		if err := msg.Decode(&txs); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		for i, tx := range txs {
			// Validate and mark the remote transaction
			if tx == nil {
				return errResp(ErrDecode, "transaction %d is nil", i)
			}
			p.MarkTransaction(tx.Hash())
		}
		//here is the injection
		pm.txpool.AddRemotes(txs)

	case msg.Code == TxListMsg:
		// decode txlist
		var txl = new(types.TxList)
		if err := msg.Decode(txl); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}

		var puk []byte
		peerOfNode := pm.peerPool.PM().GetNode(pm.coreCache.ShardID, txl.SenderInShardID)
		if peerOfNode != nil {
			puk = peerOfNode.PublicKey()
			if err := txl.VerifyTxList(puk); err != nil {
				return errResp(ErrDecode, "cannot verify txlist")
			}
			if pm.coreCache.Role == member {
				go pm.HandleTxList(txl)
			}
		} else {
			return errResp(ErrNoPukFound, "cannot find public key")
		}

		//
	case msg.Code == TxListDecMsg:

		if pm.coreCache.Role == leader {
			var txldec = new(types.TransactionListDecision)
			if err := msg.Decode(txldec); err != nil {
				return errResp(ErrDecode, "msg %v: %v", msg, err)
			}

			peerOfNode := pm.peerPool.PM().GetNode(pm.coreCache.ShardID, txldec.SenderInShardID)
			if peerOfNode != nil {
				if passed := txldec.VerifyTxListDec(peerOfNode.PublicKey()[:], pm.coreCache.ShardID); passed != true {
					return errResp(ErrDecode, "cannot verify txlist decision")
				}
				pm.coreCache.TxlDecRecvCh <- txldec

				go pm.HandleDecByLeader()
			} else {
				return errResp(ErrNoPukFound, "cannot find public key")
			}

		}
	case msg.Code == TxListDecSetInShardMsg:
		var tds = new(types.TxDecSet)
		if err := msg.Decode(tds); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		peerOfNode := pm.peerPool.PM().GetNode(tds.ShardIndexFrom, tds.SenderInShardID)
		if peerOfNode != nil {
			if passed := tds.VerifySig(peerOfNode.PublicKey()[:]); passed != true {
				return errResp(ErrDecode, "cannot verify txlist decision set")
			}

			if pm.coreCache.Role == leader {
				go pm.HandleTxDecSetLeader(tds)
			} else {
				go pm.HandleTxDecSet(tds, sameshard)
			}
		} else {
			return errResp(ErrNoPukFound, "cannot find public key")
		}

	case msg.Code == TxListDecSetXShardMsg:
		var tds = new(types.TxDecSet)
		if err := msg.Decode(tds); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}

		peerOfNode := pm.peerPool.PM().GetNode(tds.ShardIndexFrom, tds.SenderInShardID)
		if peerOfNode != nil {
			if passed := tds.VerifySig(peerOfNode.PublicKey()[:]); passed != true {
				return errResp(ErrDecode, "cannot verify txlist decision set")
			}
			go pm.HandleAndSendTxDecSet(tds)
		} else {
			return errResp(ErrNoPukFound, "cannot find public key")
		}

	case msg.Code == TxListDecSetRecv:
		var tdsr = new(types.TxDecSetRev)
		if err := msg.Decode(tdsr); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		go pm.HandleTxDecRecv(tdsr)

	case msg.Code == TxBlockMsg:
		var txblk = new(core.TxBlock)
		if err := msg.Decode(txblk); err != nil {
			return errResp(ErrDecode, "msg %v: %v", msg, err)
		}
		peerOfNode := pm.peerPool.PM().GetNode(txblk.ShardID, txblk.InShardID)
		if peerOfNode != nil {
			if passed := txblk.VerifySig(peerOfNode.PublicKey()[:]); passed != true {
				return errResp(ErrDecode, "cannot verify txlist decision set")
			}
			go pm.HandleTxBlock(txblk)
		} else {
			return errResp(ErrNoPukFound, "cannot find public key")
		}

	default:
		return errResp(ErrInvalidMsgCode, "%v", msg.Code)
	}
	return nil
}

func (pm *ProtocolManager) BroadcastTxs(txs types.Transactions) {
	var txset = make(map[*peer]types.Transactions)

	// Broadcast transactions to a batch of peers not knowing about it
	for _, tx := range txs {
		peers := pm.peers.PeersWithoutTx(tx.Hash())
		for _, peer := range peers {
			txset[peer] = append(txset[peer], tx)
		}
		log.Trace("Broadcast transaction", "hash", tx.Hash(), "recipients", len(peers))
	}
	for peer, txs := range txset {
		peer.AsyncSendTransactions(txs)
	}

}

func (pm *ProtocolManager) InShardBroadcastTx(tx *types.Transaction) {
	peers := pm.peerPool.PM().GetPeers(pm.coreCache.ShardID)
	if peers == nil {
		return
	}
	for _, p := range peers {
		peer, ok := convert(p)
		if ok {
			peer.AsyncSendTransaction(tx)
		}
	}
}

func (pm *ProtocolManager) CrossShardRelayTx(tx *types.Transaction, whereToGo uint32) {
	p := pm.peerPool.PM().GetRandom(whereToGo)
	if p == nil {
		return
	}
	peer, ok := convert(p)
	if !ok {
		return
	}
	peer.AsyncSendTransaction(tx)
}

func (pm *ProtocolManager) txBroadcastLoop() {
	lastMakeListTime := time.Now()
	for {
		select {
		case event := <-pm.txsCh:
			for _, tx := range event.Txs {
				if tx.ShardId == pm.coreCache.ShardID {
					pm.InShardBroadcastTx(tx)
				} else {
					pm.CrossShardRelayTx(tx, tx.ShardId)
				}
				pm.CrossShardRelayTx(tx, tx.GetOutputRelatedShardIdx())
				pm.coreCache.CacheTxs = append(pm.coreCache.CacheTxs, tx)
				fmt.Println("length of CacheTxs :", len(pm.coreCache.CacheTxs))
			}
		case <-time.After(time.Microsecond * 100):
			if pm.coreCache.Role == leader {
				if len(pm.coreCache.CacheTxs) >= gVar.MaxNumTxsAllowedInTXL || time.Now().After(lastMakeListTime.Add(30*time.Second)) {
					if len(pm.coreCache.TxListGoroutineTokens) == 0 {
						time.Sleep(time.Second * 5)
					} else {
						pm.makeTxList()
						lastMakeListTime = time.Now()
					}
				}
			}
		case <-pm.txsSub.Err():
			return
		}
	}
}

func (pm *ProtocolManager) HandleTxList(txl *types.TxList) error {
	statusCheck := types.StatusCheck{UnknownTxNum: gVar.INITTIAL_STATUS, Valid: nil}
	pm.PreprocessTxList(txl, &statusCheck)
	txldec, err := pm.makeTransactionListDecision(txl)
	if err == nil {
		pm.sendTransactionListDecision(txldec)
	}
	return nil
}

func (pm *ProtocolManager) PreprocessTxList(txl *types.TxList,
	statuscheck *types.StatusCheck) error {

	if statuscheck == nil {
		return fmt.Errorf("statuscheck is nil")
	}

	if statuscheck.UnknownTxNum == gVar.INITTIAL_STATUS {
		p := pm.peerPool.PM().GetLeader(pm.coreCache.ShardID)
		peer, ok := convert(p)

		if ok && peer.InShardID != txl.SenderInShardID {
			return fmt.Errorf("PreTxList: Txlist from a miner")
		}
		if int(txl.TxCnt) != len(txl.TxArray) {
			return fmt.Errorf("PreTxList: Number of Tx wrong")
		}
		statuscheck.UnknownTxNum = int(txl.TxCnt)
		statuscheck.Valid = make([]int, txl.TxCnt)
		txl.TxArray = make([]common.Hash, txl.TxCnt)

		for i := uint32(0); i < txl.TxCnt; i++ {
			_, ok := pm.coreCache.TxHashCache[txl.TxArray[i]]
			if !ok {
				tmpWait, okW := pm.coreCache.WaitHashCache[txl.TxArray[i]]
				if okW {
					tmpWait.DataTL = append(tmpWait.DataTL, txl)
					tmpWait.StatTL = append(tmpWait.StatTL, statuscheck)
					tmpWait.IDTL = append(tmpWait.IDTL, int(i))
				} else {
					tmpWait = shard.WaitProcess{nil, nil, nil, nil, nil, nil, nil, nil, nil}
					tmpWait.DataTL = append(tmpWait.DataTL, txl)
					tmpWait.StatTL = append(tmpWait.StatTL, statuscheck)
					tmpWait.IDTL = append(tmpWait.IDTL, int(i))
				}
				pm.coreCache.WaitHashCache[txl.TxArray[i]] = tmpWait
			} else {
				statuscheck.UnknownTxNum--
				tmp := pm.coreCache.CacheXshardDecs.Get(txl.TxArray[i])
				if tmp == nil {
					fmt.Println("TXcache not ok! hash:", txl.TxArray[i])
				} else if tmp.Data == nil {
					fmt.Println("Tx Data is null")
				}
				statuscheck.Valid[i] = gVar.TX_KNOWN
			}
		}
	}

	if statuscheck.UnknownTxNum > 0 {
		statuscheck.Channel = make(chan bool, statuscheck.UnknownTxNum)
	}

	return nil
}

func (pm *ProtocolManager) makeTxList() {
	<-pm.coreCache.TxListGoroutineTokens
	pm.coreCache.Mutx.Lock()
	tlg, _ := pm.NewTLGInstance()
	for _, tx := range pm.coreCache.CacheTxs {
		tmpXsd := pm.coreCache.CacheXshardDecs.Get(tx.GetHash())
		ok := yes
		if tmpXsd == nil {
			ok = no
			tmpXsd = new(types.CrossShardDec)
			tmpXsd.New(tx)
		} else {
			tmpXsd.Update(tx)
		}

		// still unknown
		if tmpXsd.Check[pm.coreCache.ShardID] == gVar.DECISION_UNKNOWN {
			if ok == yes {
				pm.coreCache.CacheXshardDecs.Delete(tx.GetHash())
			}
			// not related tx, continue
			continue
		}

		if tmpXsd.Res == gVar.RESULT_YES {
			pm.coreCache.Ready = append(pm.coreCache.Ready, *tmpXsd.Data)
		}
		// record
		pm.coreCache.CacheXshardDecs.Set(tx.GetHash(), tmpXsd)

		// add tx to related list, the tls[i] will only contains txs related to the same shard
		for _, i := range tmpXsd.ShardRelated {
			tlg.TLS[i].AddTx(tx)
		}
		ch := make(chan uint32, gVar.ShardSize)
		pm.coreCache.TxlDeChanMap.Set(tlg.TLS[0].Round, ch)

	}

	pm.coreCache.TxListGroupPool <- tlg
	pm.coreCache.Mutx.Unlock()
	// reset the cache
	if len(pm.coreCache.CacheTxs) != 0 {
		pm.coreCache.CacheTxs = make([]*types.Transaction, 0)
	}

}

// generate a new TLG
func (pm *ProtocolManager) NewTLGInstance() (*shard.TLGroup, error) {

	inst := new(shard.TLGroup)
	pm.coreCache.TLRoundLock.Lock()
	for i := uint32(0); i < gVar.ShardCnt; i++ {
		inst.TDS[i] = new(types.TxDecSet)
		inst.TLS[i] = new(types.TxList)
		inst.TLS[i].SenderInShardID = pm.coreCache.ShardID
		inst.TLS[i].Round = pm.coreCache.TLRound
		pm.coreCache.TLRound++
	}
	pm.coreCache.TLRoundLock.Unlock()
	return inst, nil
}

func (pm *ProtocolManager) makeTransactionListDecision(txl *types.TxList) (*types.TransactionListDecision, error) {
	pm.coreCache.TLRoundLock.Lock()
	txldec := new(types.TransactionListDecision)
	txldec.Set(pm.coreCache.InShardID, pm.coreCache.ShardID, true)
	txldec.TxlistHash = txl.HashID
	pm.coreCache.TxCnt += txl.TxCnt

	var tmpDecision [gVar.ShardCnt]types.TransactionListDecision
	for i := uint32(0); i < gVar.ShardCnt; i++ {
		tmpDecision[i].Set(pm.coreCache.InShardID, i, false)
	}

	for _, hash := range txl.TxArray {
		// if the hash is found in map
		for {
			pm.coreCache.CacheTxsRWLock.RLock()
			xsd := pm.coreCache.CacheXshardDecs.Get(hash)
			if xsd != nil {
				var res byte
				if xsd.Check[pm.coreCache.ShardID] == gVar.DECISION_RELATED_NO_RES {
					if true {
						res = byte(1)
					}
					txldec.Add(res)
					for j := 0; j < len(xsd.ShardRelated); j++ {
						tmpDecision[xsd.ShardRelated[j]].Add(res)
					}
				}

				pm.coreCache.CacheTxsRWLock.RUnlock()

				break
			} else {
				txldec.Add(0)
				pm.coreCache.CacheTxsRWLock.RUnlock()
			}
			time.Sleep(time.Millisecond * 20)
		}
	}

	for i := uint32(0); i < gVar.ShardCnt; i++ {
		tmpDecision[i].SignTxListDec(pm.coreCache.Prikey, 0)
		txldec.Sigs[i] = tmpDecision[i].Sigs[0]
	}

	pm.coreCache.TLRound++
	pm.coreCache.TLRoundLock.Unlock()
	return txldec, nil
}

// shard member sends transaction list decision
func (pm *ProtocolManager) sendTransactionListDecision(txldec *types.TransactionListDecision) {

	if txldec != nil {
		p := pm.peerPool.PM().GetLeader(pm.coreCache.ShardID)
		if p != nil {
			peer, ok := convert(p)
			if ok {
				peer.SendTransactionListDecision(txldec)
			}
		}
	}
}

// handle txlist decision coming from shard members
func (pm *ProtocolManager) HandleDecByLeader() {
	var listSequence uint32
	txldec := <-pm.coreCache.TxlDecRecvCh

	pm.UpdateDecSetByLeader(txldec, &listSequence)
	tC := pm.coreCache.TxlDeChanMap.Get(listSequence)
	if tC != nil {
		tC <- txldec.SenderInShardID
	}

}

func (pm *ProtocolManager) UpdateDecSetByLeader(txldec *types.TransactionListDecision,
	sequence *uint32) error {

	if txldec.MultiShardsSigFlag == false {
		return fmt.Errorf("TxDecision parameter error")
	}
	tmpTLG, ok := pm.coreCache.TlgMap[txldec.TxlistHash]
	if !ok {
		//index = nil
		return fmt.Errorf("TxDecision Hash error, wrong or time out")
	}

	*sequence = tmpTLG.TLS[pm.coreCache.ShardID].Round - pm.coreCache.PrevHeight

	tmpTDs := make([]types.TransactionListDecision, gVar.ShardCnt)

	for i := uint32(0); i < gVar.ShardCnt; i++ {

		tmpTDs[i].Set(0, i, false)
		tmpTDs[i].TxlistHash = tmpTLG.TLS[i].HashID
		tmpTDs[i].Sigs = nil
		tmpTDs[i].Sigs = append(tmpTDs[i].Sigs, txldec.Sigs[i])
	}

	var x, y uint32 = 0, 0
	for i := uint32(0); i < tmpTLG.TLS[pm.coreCache.ShardID].TxCnt; i++ {
		tmpCrossShardDec := pm.coreCache.CacheXshardDecs.Get(tmpTLG.TLS[pm.coreCache.ShardID].TxArray[i])
		if tmpCrossShardDec == nil {
			fmt.Println("Not related tx?")
		}
		for j := 0; j < len(tmpCrossShardDec.ShardRelated); j++ {
			tmpTDs[tmpCrossShardDec.ShardRelated[j]].Add((txldec.Decision[x] >> y) & 1)
		}
		if y < 7 {
			y++
		} else {
			x++
			y = 0
		}
	}

	for i := uint32(0); i < gVar.ShardCnt; i++ {
		var pukTheOneWhoOffersTxDec []byte
		if !tmpTDs[i].VerifyTxListDec(pukTheOneWhoOffersTxDec, 0) {
			return fmt.Errorf("Signature not match %d", i)
		}
	}
	for i := uint32(0); i < gVar.ShardCnt; i++ {
		tmpTLG.TDS[i].AddDecision(&tmpTDs[i])
	}

	return nil
}

// get object from interface
func convert(in interface{}) (*peer, bool) {
	p, ok := in.(*peer)
	return p, ok
}
