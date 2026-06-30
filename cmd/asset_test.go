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

const (
	AssetAmtOnionType     uint64 = 65536
	AssetIDOnionType      uint64 = 65537
	AssetFwdFlagOnionType uint64 = 65539
)

func main() {
	if len(os.Args) < 5 {
		fmt.Println("Usage: asset_test <lnd_host:port> <tls_cert> <macaroon> <payment_hash_hex>")
		os.Exit(1)
	}

	lndHost := os.Args[1]
	tlsCertPath := os.Args[2]
	macaroonPath := os.Args[3]
	paymentHashHex := os.Args[4]

	creds, err := credentials.NewClientTLSFromFile(tlsCertPath, "")
	if err != nil {
		fmt.Printf("TLS error: %v\n", err)
		os.Exit(1)
	}

	macBytes, err := os.ReadFile(macaroonPath)
	if err != nil {
		fmt.Printf("Macaroon error: %v\n", err)
		os.Exit(1)
	}

	mac := &macaroon.Macaroon{}
	if err := mac.UnmarshalBinary(macBytes); err != nil {
		fmt.Printf("Macaroon unmarshal error: %v\n", err)
		os.Exit(1)
	}

	cred, err := macaroons.NewMacaroonCredential(mac)
	if err != nil {
		fmt.Printf("Macaroon credential error: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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

	lnClient := lnrpc.NewLightningClient(conn)
	routerClient := routerrpc.NewRouterClient(conn)

	info, err := lnClient.GetInfo(ctx, &lnrpc.GetInfoRequest{})
	if err != nil {
		fmt.Printf("GetInfo error: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Connected to: %s\n", info.IdentityPubkey)

	assetIDHex := "419cb727f0c9569bd2f62bb8dddc32b0ccd2f6d0513518ec5b5417ae60dba29d"
	assetID, _ := hex.DecodeString(assetIDHex)
	node4Key := "0308b60f3ff70e892b4e911a0b10d70be6dfb72d7a08550a243686db28e8662b8f"

	paymentHash, err := hex.DecodeString(paymentHashHex)
	if err != nil {
		fmt.Printf("Payment hash decode error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n=== Step 1: Query Route (3 hops to node4) ===")
	queryReq := &lnrpc.QueryRoutesRequest{
		PubKey: node4Key,
		Amt:    500,
	}
	routeResp, err := lnClient.QueryRoutes(ctx, queryReq)
	if err != nil {
		fmt.Printf("QueryRoutes error: %v\n", err)
		os.Exit(1)
	}
	if len(routeResp.Routes) == 0 {
		fmt.Println("No routes found")
		os.Exit(1)
	}

	route := routeResp.Routes[0]
	fmt.Printf("Route: %d hops\n", len(route.Hops))
	for i, hop := range route.Hops {
		fmt.Printf("  Hop %d: chan=%d (%s), amt=%d msat, pubkey=%s\n",
			i, hop.ChanId, hop.ChanId, hop.AmtToForwardMsat, hop.PubKey)
	}

	fmt.Println("\n=== Step 2: Inject Asset Forwarding TLV ===")
	assetAmt := uint64(500000) // 0.5 asset units

	for i := 0; i < len(route.Hops)-1; i++ {
		if route.Hops[i].CustomRecords == nil {
			route.Hops[i].CustomRecords = make(map[uint64][]byte)
		}
		encodedAmt := uint64ToTLVBytes(assetAmt)
		route.Hops[i].CustomRecords[AssetAmtOnionType] = encodedAmt
		route.Hops[i].CustomRecords[AssetIDOnionType] = assetID
		route.Hops[i].CustomRecords[AssetFwdFlagOnionType] = []byte{0x01}
		fmt.Printf("  Hop %d (intermediate): AssetAmt=%d, FwdFlag=1\n", i, assetAmt)
	}

	lastIdx := len(route.Hops) - 1
	if route.Hops[lastIdx].CustomRecords == nil {
		route.Hops[lastIdx].CustomRecords = make(map[uint64][]byte)
	}
	encodedAmt := uint64ToTLVBytes(assetAmt)
	route.Hops[lastIdx].CustomRecords[AssetAmtOnionType] = encodedAmt
	route.Hops[lastIdx].CustomRecords[AssetIDOnionType] = assetID
	fmt.Printf("  Hop %d (final): AssetAmt=%d\n", lastIdx, assetAmt)

	// Extract payment_addr from the invoice's payment request
	payReq, err := lnClient.DecodePayReq(ctx, &lnrpc.PayReqString{
		PayReq: os.Args[5],
	})
	if err == nil && payReq != nil {
		route.Hops[lastIdx].MppRecord = &lnrpc.MPPRecord{
			PaymentAddr:  payReq.PaymentAddr,
			TotalAmtMsat: 500000,
		}
		fmt.Printf("  Added MPP record: payment_addr=%x, total=%d msat\n",
			payReq.PaymentAddr, 500000)
	} else {
		fmt.Printf("  Could not decode pay req: %v\n", err)
	}

	fmt.Println("\n=== Step 3: SendToRouteV2 ===")
	sendResp, err := routerClient.SendToRouteV2(ctx, &routerrpc.SendToRouteRequest{
		PaymentHash: paymentHash,
		Route:       route,
	})
	if err != nil {
		fmt.Printf("SendToRouteV2 error: %v\n", err)
	} else {
		fmt.Printf("SendToRouteV2 response:\n")
		fmt.Printf("  Status: %v\n", sendResp.Status)
		fmt.Printf("  FailureCode: %d\n", sendResp.FailureCode)
		if sendResp.FailureCode != 0 {
			fmt.Printf("  FailureReason: route=%d\n", sendResp.FailureCode)
		}
	}
}

func uint64ToTLVBytes(v uint64) []byte {
	b := make([]byte, 8)
	b[0] = byte(v >> 56)
	b[1] = byte(v >> 48)
	b[2] = byte(v >> 40)
	b[3] = byte(v >> 32)
	b[4] = byte(v >> 24)
	b[5] = byte(v >> 16)
	b[6] = byte(v >> 8)
	b[7] = byte(v)
	zeros := 0
	for zeros < 7 && b[zeros] == 0 {
		zeros++
	}
	return b[zeros:]
}
