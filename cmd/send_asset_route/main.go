package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/lightningnetwork/lnd/lnrpc"
	"github.com/lightningnetwork/lnd/lnrpc/routerrpc"
	"github.com/lightningnetwork/lnd/macaroons"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"gopkg.in/macaroon.v2"
)

func main() {
	if len(os.Args) < 7 {
		fmt.Println("Usage: send_asset_route <host:port> <tls_cert> <mac> <pay_hash> <pay_addr> <chan1> <chan2> [asset_amt]")
		os.Exit(1)
	}

	lndHost := os.Args[1]
	tlsCert := os.Args[2]
	macPath := os.Args[3]
	payHashHex := os.Args[4]
	payAddrHex := os.Args[5]
	chan1 := os.Args[6]
	chan2 := os.Args[7]

	assetAmt := uint64(500000) // 0.5 units (6 decimal)
	if len(os.Args) > 8 {
		fmt.Sscanf(os.Args[8], "%d", &assetAmt)
	}

	creds, _ := credentials.NewClientTLSFromFile(tlsCert, "")
	macBytes, _ := os.ReadFile(macPath)
	mac := &macaroon.Macaroon{}
	mac.UnmarshalBinary(macBytes)
	cred, _ := macaroons.NewMacaroonCredential(mac)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, lndHost,
		grpc.WithTransportCredentials(creds),
		grpc.WithPerRPCCredentials(cred),
		grpc.WithBlock(),
	)
	if err != nil {
		fmt.Printf("Connect error: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	router := routerrpc.NewRouterClient(conn)

	payHash, _ := hex.DecodeString(payHashHex)
	payAddr, _ := hex.DecodeString(payAddrHex)

	assetID, _ := hex.DecodeString("419cb727f0c9569bd2f62bb8dddc32b0ccd2f6d0513518ec5b5417ae60dba29d")

	var cid1, cid2 uint64
	fmt.Sscanf(chan1, "%d", &cid1)
	fmt.Sscanf(chan2, "%d", &cid2)

	// TLV truncated uint64 encoding
	amtBytes := make([]byte, 8)
	amtBytes[0] = byte(assetAmt >> 56)
	amtBytes[1] = byte(assetAmt >> 48)
	amtBytes[2] = byte(assetAmt >> 40)
	amtBytes[3] = byte(assetAmt >> 32)
	amtBytes[4] = byte(assetAmt >> 24)
	amtBytes[5] = byte(assetAmt >> 16)
	amtBytes[6] = byte(assetAmt >> 8)
	amtBytes[7] = byte(assetAmt)
	z := 0
	for z < 7 && amtBytes[z] == 0 {
		z++
	}
	amtEncoded := amtBytes[z:]

	route := &lnrpc.Route{
		TotalAmt:     502,
		TotalAmtMsat: 502000,
		TotalFees:    2,
		TotalFeesMsat: 2000,
		TotalTimeLock: 200,
		Hops: []*lnrpc.Hop{
			{
				ChanId:           cid1,
				AmtToForward:     501,
				AmtToForwardMsat: 501000,
				Expiry:           200,
				PubKey:           "02c587bea0bc23885ada9bc00b02e5786ccdff5d60be7b7bae746d7c2d95283218",
				CustomRecords: map[uint64][]byte{
					65536: amtEncoded,
					65537: assetID,
					65539: {0x01},
				},
			},
			{
				ChanId:           cid2,
				AmtToForward:     500,
				AmtToForwardMsat: 500000,
				Expiry:           140,
				MppRecord: &lnrpc.MPPRecord{
					TotalAmtMsat: 500000,
					PaymentAddr:  payAddr,
				},
				CustomRecords: map[uint64][]byte{
					65536: amtEncoded,
					65537: assetID,
				},
			},
		},
	}

	fmt.Printf("Sending pure asset route:\n")
	fmt.Printf("  Hop0: chan=%d TLV=[65536(asset_amt=%d),65537(asset_id),65539(fwd_flag=1)]\n", cid1, assetAmt)
	fmt.Printf("  Hop1: chan=%d TLV=[65536(asset_amt=%d),65537(asset_id)]\n", cid2, assetAmt)

	resp, err := router.SendToRouteV2(ctx, &routerrpc.SendToRouteRequest{
		PaymentHash: payHash,
		Route:       route,
	})
	if err != nil {
		fmt.Printf("SendToRouteV2 error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Status: %v\n", resp.Status)
	if resp.Failure != nil {
		fmt.Printf("Failure: code=%d source=%d\n", resp.Failure.Code, resp.Failure.FailureSourceIndex)
	}
	if resp.Status == lnrpc.HTLCAttempt_SUCCEEDED {
		fmt.Println("PAYMENT SUCCEEDED")
	} else {
		fmt.Println("PAYMENT FAILED (may be expected if invoice not set up)")
	}
}
