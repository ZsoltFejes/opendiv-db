package opendivdb

import (
	"bytes"
	"crypto/aes"
	"fmt"
)

func EncryptAES(key []byte, data []byte) ([]byte, error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("Unable to create AES Cipher! " + err.Error())
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

	return bytes.Trim(out, "\x00"), nil
}

func DecryptAES(key []byte, ciphertext []byte) ([]byte, error) {

	c, err := aes.NewCipher(key)
	if err != nil {
		return ciphertext, fmt.Errorf("Unable to create AES Cipher " + err.Error())
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

	return bytes.Trim(pt, "\x00"), nil
}
