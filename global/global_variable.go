package gVar

import "time"

//MagicNumber magic
const MagicNumber byte = 66

//ShardSize is the number of miners in one shard
const ShardSize uint32 = 225

//ShardCnt is the number of shards
const ShardCnt uint32 = 8

//used in rep calculation, scaling factor
const RepTP = 1
const RepTN = 1
const RepFP = 0
const RepFN = 0

//channel

const SlidingWindows = 20

//NumTxListPerEpoch is the number of txblocks in one epoch
const NumTxListPerEpoch = 10 //60

//NumTxBlockForRep is the number of blocks for one rep block
const NumTxBlockForRep = 4 //10

const NumberRepPerEpoch = NumTxListPerEpoch/NumTxBlockForRep + 1

//const GensisAcc = []byte{0}

const GensisAccValue = 2147483647

const TxSendInterval = 10

const NumOfTxForTest = 100

const GeneralSleepTime = 50

var T1 time.Time = time.Now()

const BandDiverse = true

const MyAddress = "172.18.21.130:9999"

const MaxBand = 38 * 1024
const MinBand = 2 * 1024

const ExperimentBadLevel = 0
