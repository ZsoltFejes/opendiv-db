package main

import (
	"bytes"
	"crypto/aes"
	"encoding/hex"
)

func EncryptAES(key string, data []byte) string {
	keyBytes := []byte(key)
	c, err := aes.NewCipher(keyBytes)
	if err != nil {
		l("Unable to create AES Cipher! "+err.Error(), true, true)
	}
	out := make([]byte, 1048576)

	passes := len(data) / 16
	if len(data) > passes*16 {
		passes++
	}
	for i := 0; i < passes; i++ {
		pass := make([]byte, 16)
		data_to_encrypt := make([]byte, 16)
		offset := 16 * i
		bytes_left := len(data[offset:])

		if bytes_left > 16 {
			for b := 0; b < 16; b++ {
				data_to_encrypt[b] = data[offset+b]
			}
		} else {
			for b := 0; b < len(data[offset:]); b++ {
				data_to_encrypt[b] = data[offset+b]
			}
		}

		c.Encrypt(pass, data_to_encrypt)
		for b := 0; b < 16; b++ {
			out[offset+b] = pass[b]
		}
	}

	return hex.EncodeToString(bytes.Trim(out, "\x00"))
}

func DecryptAES(key string, ct string) []byte {
	keyBytes := []byte(key)
	ciphertext, err := hex.DecodeString(ct)
	if err != nil {
		l("Unable to convert hex to cypher text! "+err.Error(), false, true)
	}

	c, err := aes.NewCipher(keyBytes)
	if err != nil {
		l("Unable to create AES Cipher "+err.Error(), true, true)
	}

	pt := make([]byte, 1048576)

	passes := len(ciphertext) / 16
	if len(ciphertext) > passes*16 {
		passes++
	}
	for i := 0; i < passes; i++ {
		pass := make([]byte, 16)
		data_to_decrypt := make([]byte, 16)
		offset := 16 * i
		bytes_left := len(ciphertext[offset:])

		if bytes_left > 16 {
			for b := 0; b < 16; b++ {
				data_to_decrypt[b] = ciphertext[offset+b]
			}
		} else {
			for b := 0; b < len(ciphertext[offset:]); b++ {
				data_to_decrypt[b] = ciphertext[offset+b]
			}
		}

		c.Decrypt(pass, data_to_decrypt)
		for b := 0; b < 16; b++ {
			pt[offset+b] = pass[b]
		}
	}

	return bytes.Trim(pt, "\x00")
}
