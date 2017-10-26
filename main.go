package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/pborman/uuid"
	btrdb "gopkg.in/btrdb.v4"
)

func main() {
	if len(os.Args) < 7 {
		fmt.Printf("Usage: %s src_uuid dst_uuid num_copies start_delta spacing_nanos hostname:port...\n", os.Args[0])
		os.Exit(1)
	}

	srcUUID := uuid.Parse(os.Args[1])
	if srcUUID == nil {
		fmt.Printf("%s is not a valid UUID\n", os.Args[1])
		os.Exit(2)
	}

	dstUUID := uuid.Parse(os.Args[2])
	if dstUUID == nil {
		fmt.Printf("%s is not a valid UUID\n", os.Args[2])
		os.Exit(3)
	}

	copies, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil {
		fmt.Printf("%s is not a valid integer\n", os.Args[3])
		os.Exit(4)
	}

	delta, err := strconv.ParseInt(os.Args[4], 10, 64)
	if err != nil {
		fmt.Printf("%s is not a valid integer\n", os.Args[4])
		os.Exit(5)
	}

	spacing, err := strconv.ParseInt(os.Args[5], 10, 64)
	if err != nil {
		fmt.Printf("%s is not a valid integer\n", os.Args[5])
		os.Exit(6)
	}

	b, err := btrdb.Connect(context.Background(), os.Args[6:]...)
	if err != nil {
		fmt.Println(err)
		os.Exit(7)
	}

	src := b.StreamFromUUID(srcUUID)
	dst := b.StreamFromUUID(dstUUID)

	CopyStream(context.Background(), src, dst, copies, delta, spacing)

	fmt.Println("All done.")
}
