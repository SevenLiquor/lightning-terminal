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
	if len(os.Args) < 9 {
		fmt.Println("Usage: sar2 <host> <tls> <mac> <hash> <addr> <chan> <pk> [amt]")
		os.Exit(1)
	}

	lndHost := os.Args[1]
	tlsCert := os.Args[2]
	macPath := os.Args[3]
	payHashHex := os.Args[4]
	payAddrHex := os.Args[5]
	chanStr := os.Args[6]
	pk := os.Args[7]

	assetAmt := uint64(500000)
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

	var cid uint64
	fmt.Sscanf(chanStr, "%d", &cid)

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
		TotalAmt:     500,
		TotalAmtMsat: 500000,
		TotalFees:    0,
		TotalFeesMsat: 0,
		TotalTimeLock: 126830,
		Hops: []*lnrpc.Hop{
			{
				ChanId:           cid,
				AmtToForward:     500,
				AmtToForwardMsat: 500000,
				Expiry:           126830,
				PubKey:           pk,
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

	fmt.Printf("Sending 1-hop: chan=%d → %s, amt=%d\n", cid, pk[:20], assetAmt)

	// Wire-level asset TLV for aux_traffic_shaper interception
	firstHopRecords := map[uint64][]byte{
		65536: amtEncoded,
		65537: assetID,
		65542: {0x01},
	}

	resp, err := router.SendToRouteV2(ctx, &routerrpc.SendToRouteRequest{
		PaymentHash:           payHash,
		Route:                 route,
		FirstHopCustomRecords: firstHopRecords,
	})
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Status: %v\n", resp.Status)
	if resp.Failure != nil {
		fmt.Printf("Failure: code=%d source=%d\n",
			resp.Failure.Code, resp.Failure.FailureSourceIndex)
	}
	if resp.Status == lnrpc.HTLCAttempt_SUCCEEDED {
		fmt.Println("PAYMENT SUCCEEDED")
	}
}
