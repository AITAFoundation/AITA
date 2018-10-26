package cryptonew

import (
	"crypto/ecdsa"
	"crypto/sha256"
)

//GenerateAddr generate the address based on public key
func GenerateAddr(puk ecdsa.PublicKey) [32]byte {
	tmp := append(puk.X.Bytes(), puk.Y.Bytes()...)
	newHash := sha256.Sum256(tmp)
	return newHash
}

//AddressGenerate Wallet address generation
func AddressGenerate(priv *ecdsa.PrivateKey) [32]byte {

	tmp := priv.PublicKey
	return GenerateAddr(tmp)
}

//Verify verify the address with the public key
func Verify(puk ecdsa.PublicKey, addr [32]byte) bool {
	tmp := GenerateAddr(puk)
	return tmp == addr
}
