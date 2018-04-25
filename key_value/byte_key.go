package key_value

import "encoding/base64"

type Key []byte

func (k Key) B64() (string){
	return base64.StdEncoding.EncodeToString(k)
}

func (k Key) Bucket(numBuckets int) int {
	var tempByte byte;

	for _, b := range k {
		tempByte = tempByte ^ b
	}
	bucket := int(tempByte) % numBuckets
	return bucket
}

func KeyFromB64(k string) Key {
	kb, err := base64.StdEncoding.DecodeString(k)

	if err == nil {
		return kb
	}
	return nil
}