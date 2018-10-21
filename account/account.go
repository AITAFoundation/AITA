package account

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"

	"math/big"

	"RC/base58"
	"RC/cryptonew"
	"RC/ed25519"
)

//RcAcc the wallet of a user1
type RcAcc struct {
	Pri      ecdsa.PrivateKey
	Puk      ecdsa.PublicKey
	CosiPri  ed25519.PrivateKey
	CosiPuk  ed25519.PublicKey
	Addr     string
	AddrReal [32]byte //public key -> id
	//AccType  int
	ID  string
	Rep int //total reputation among a period of time
}

//New generate a new wallet with different type
func (acc *RcAcc) New(ID string) {
	h := sha256.New()
	h.Write([]byte(ID))
	var curve elliptic.Curve
	curve = elliptic.P256()
	var tmp *ecdsa.PrivateKey
	tmp, _ = ecdsa.GenerateKey(curve, rand.Reader)
	acc.Pri = *tmp
	acc.Puk = acc.Pri.PublicKey
	acc.AddrReal = cryptonew.AddressGenerate(&acc.Pri)
	acc.Addr = base58.Encode(acc.AddrReal[:])
	//acc.AccType = accType
}

//NewCosi xx
func (acc *RcAcc) NewCosi() {
	pubKey, priKey, _ := ed25519.GenerateKey(nil)
	acc.CosiPri = priKey
	acc.CosiPuk = pubKey
}

//Load loading the account information
func (acc *RcAcc) Load(a1, a2, a3, a4, a5 string) {
	acc.Pri.Curve = elliptic.P256()
	acc.Pri.D = new(big.Int)
	acc.Pri.D.SetString(a1, 10)
	acc.Pri.X = new(big.Int)
	acc.Pri.X.SetString(a2, 10)
	acc.Pri.Y = new(big.Int)
	acc.Pri.Y.SetString(a3, 10)
	acc.Puk = acc.Pri.PublicKey
	acc.Addr = a4
	acc.AddrReal = cryptonew.AddressGenerate(&acc.Pri)
	//acc.AccType, _ = strconv.Atoi(a5)
}

//RetPri return the private key
func (acc *RcAcc) RetPri() ecdsa.PrivateKey {
	return acc.Pri
}
