package datastore

import (
	"bytes"
	"encoding/base64"
	"os"
	"testing"
)

const (
	testFile = ".test.db"

	// 2000 bytes of random data in base64
	testDataBase64 = "3EREGd0es9iKDty4JGu54oIlgpJL2Ad1gFmvehiLM/44DiI33SqQyqx2kRf9AKfNvrIMZBqyuxM0Y6QnX8FXxQFn6evuF7wUGmTqleIh6QCE+/gU+Z3dboddMfGid4OdeSVUCNNJJvja/tO6NHrd1CQ6SpZRjQVLyAuRtB527Fb/6TWX3sibS7sR81sQfNgtTAjiw7vGr5VcMIfVB3b9IjGFfN0wuEq/QuPF+A2yOzjOy9sFJrKECyslWo0va1IiBprxqh8kfWHv3wHscAO9hmxGJM8/sGCdKiyt3BHUtAcfPLQVCkperl6ce1Y8hkhwXc1HrmeiqWnzswjmzYedmhvtzb58zBprlPtB3l9a/4kyBLeRCN8fCBljOcI2ovCWhngYsAyHhYz7xEkSBee6gFzEu9vDYlyKFZaYcl2MzcGJz0S8f/KC2IbwL6Mfqks5an9ucXYc2bHXKeg3FSVD8DCdYHjbbbgz1DCybprLnL76Wo4z10zPJ7rvMJvjIoEEPAuQzebgwdgr+ydBfJ4s/rb0RLVtC98TUbZYZMt8v/m8cRGyAECGeLZiaIyIi1yCwEFLaUqwq37/oLUFCb0XHWEk39A3bGWdwnCOLYm8R2dmou79wHRkiiW891PBDGDg9wtymSvCyd+0fYVpq0GAXvymifaaahAGN+kv91lSOI8t9cEcy+5YXNWphigqHJ/66Y+29NJtWVDfTZ1IvwpttE9BN0WZXo/kZc7xWLxXlCNduA3/CLYp/l/+af2rVxo6E+jVG4LgrGdEvu62aS9KLLcoA7caT7uhKWCfwxcrOaPS2F/NV+IzGeVj4G9MLuAPvoPWFe0+0THTAVW+dJpQ0mBRraLTOsADUKID0WpMTSzEfndfUt4x6dxA+AuVfegnd8ATuJ24giPXK6+xNrQUMzw4KgOLPKT4es+J3SblmtMSE758ZlOa7K8VxRI3EqLTDf1UcQlX+alcdj0RygyOm5wDV8f8AnlYyl+DYPysN2XOETeeqg51wx3k/QeqPCIejK0OS7iwntBxMMj5OCpGssvt2AvDWGpiIEFFfm+/Aw+tcRLwwcwxP1J9VPJ1pvMftmECQpSm2eGz9hJCp0nG4OztFsMgY7/DNXvJJNPrw34VPHVVD2uJr5O/H1jlaWpFm5sUzJu99lROUSM3uB2YZaI+V60EcEdM4Mri7Ffxgtv6ekkEX5/YZXZ/RUQ752FBrAeyNWBQtCuUPr5mvKxsDj74+IjbnbnUmRJFLPn5ZChHP48M3yv3EzGTUUrvd17o0nnHK6zYlFYJhBdwirEbM+KedZct7Rj/EZ/Xtz63Cyb1Pkd0RM3uq46awZQWcAkdR4VzKAQswsBFzglBteDtD1k5v4t0we87VcNO4Xo54TUF0w6nuDBFkWEJrVn2528PHgL/qKWGTrfZWUhJtaOcA9RgujK+Dj7XASDKXEpoF7oFEsw+ZpqRV4+SAR/TCyEyiD43n/85qzMzw/9dabEzhXABbRq0GcWxtRM4dwTYJGAiDFjEalnGcww0AluJdSgeLg3WKEbAcGV4AlKr6hCe2n4fEkefMj/ZTOOfnxWc9VgeJk6dkrRrvM4kk28NopHbB8aZC4b3bGdF3//RVjSoFjpxhcFsmnCwud4u1EyvTAIl19lyeTul510mSGpn4TMMPPMtozh5N5B0LmVIs3LjSdUKlXTI2VLFgwUrcTDlOCY2/FVv2gbeTmoHcwDl8geUB9AqCZ7BYeXCoQLoZeE8aBmjs97wDMyKWI+t32J34FBhtw2cRJ0f+uoYaPpwAtvzUAVWd82/WuoDTzr/BaETnXj3jQ63GVlylzxRM81bgZacArl1hhK/H6PquJ4ou6H8L90NXj1aIxzjTifNfdX6u3xPpFw1IL++zQnJeCfL/EK3CuBZHcgWDZUqn2UebvjZyoay8tJEQ11vLRGppqLybTkhOlffSeCTLqtxVrqHcRiB5TTLHYxG77pMw4or53qwRHfBgz4sHJAOD4ug/oJ9ShLIK+MCIg8+Ln7l5Ad+AY0Yk486ZuhwNbuBRAsn1s4vgbEbqW/tW0EV/S86pXunods/vd09EeeBVKW83WbZEchYFBAVXaOF1erJtyXZbgyoqo5DeNm94YI+fx65zJ4vSo08f/3rFntXFv6kCZZtaDTAVaeKBw5+NZ+WO/bitl0z4KZuClRWIewLLBG3oxKysos0ElMftYqI4slXXndrPJfPNnqiVccB4EkhHyDBdKSKxwqHZUJI2Ly8q1pnYeZR5FDXSjqOL76Snt86DHRZebafGWW3PzdvwF2/CjM060wZT8pwlcCaJydH7nhugbxJ/coGWGABdh9ZZLVLQc1Vl3a13poITtxjyRMAbq6baKKCEinZB57kwJ/J2Ey0C4n//1taEfctF6y5D5JZrp/+4Y5MROTvX+dAZLFw4G3TTZdwPJRkScKAeDk3NJ8kZcq/TzPYpleJUiDFB/6pqSFB03qyvvMyRZHot8B7/DpIw+kYvted5rql5D2AX0ZPFkluevDEK0dmBg5SVNstlr1ap3zNlDZ6yb0wLr2weWX3rMlFzTE1lRey+57HI/adrGLI29N8FcbyMmP6V0aAjSF4Wu0uBhpktaSp/dHHzGFZTh67HMPxOXHpFR3Sn+Jn/mmhNvX394UZuN7aXuV3qm6NebI="
)

// Hold real, binary data, decoded from testDataBase64
var testData []byte

func init() {
	testData = make([]byte, base64.StdEncoding.DecodedLen(len(testDataBase64)))
	base64.StdEncoding.Decode(testData, []byte(testDataBase64))
}

func testCleanUp() {
	os.Remove(testFile)
}

func TestSequentialWriteAndRead(t *testing.T) {
	defer testCleanUp()

	var (
		store *FileStore
		err   error
		id    Id
		data  []byte
	)

	if store, err = NewFileStore(testFile); err != nil {
		t.Error("Could not create file store")
	}

	if id, err = store.Write(testData); err != nil {
		t.Error("Could not write data to store")
	}

	if data, err = store.Read(id); err != nil {
		t.Error("Could not read data from store")
	}

	if !bytes.Equal(data, testData) {
		t.Error("Written/Read data doesn't match")
	}
}

func BenchmarkReadsAndWrite(b *testing.B) {
	// defer testCleanUp()

	var (
		store *FileStore
	)

	store, _ = NewFileStore(testFile)

	for i := 0; i < b.N; i++ {
		store.Write(testData)
	}
}
