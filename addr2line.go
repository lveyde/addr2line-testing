package main

import (
	"database/sql"
	"fmt"
	addr2line "github.com/elazarl/addr2line"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
	"os"
	"strconv"
	"sync"
)

var (
	wg          sync.WaitGroup
	maxRequests uint64 = 3000
)

// Sql connection configuration
type Connect_token struct {
	DBDriver string
	DBDSN    string
}

// Connects the target db and returns the handle
func Connect_db(t *Connect_token) *sql.DB {
	//fmt.Println("connect")
	db, err := sql.Open((*t).DBDriver, (*t).DBDSN)
	if err != nil {
		panic(err)
	}
	//fmt.Println("connected")
	return db
}

type Symbols struct {
	symbolName    string
	symbolAddress string
}

func getSymbols(context *Context, instanceID string) ([]Symbols, error) {
	query := fmt.Sprintf("SELECT symbol_name, symbol_address FROM symbols WHERE symbol_instance_id_ref = %s", instanceID)
	//fmt.Println("Running query", query)
	rows, err := (*context).DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// A symbols slice to hold data from returned rows.
	var symbols []Symbols

	// Loop through rows, using Scan to assign column data to struct fields.
	for rows.Next() {
		var symbol Symbols
		if err := rows.Scan(&symbol.symbolName, &symbol.symbolAddress); err != nil {
			return symbols, err
		}
		symbols = append(symbols, symbol)
	}
	if err = rows.Err(); err != nil {
		return symbols, err
	}
	return symbols, nil
}

// Context type
type Context struct {
	a2l         *addr2line.Addr2line
	ch_workload chan Workload
	mu          sync.Mutex
	DB          *sql.DB
}

type Workload struct {
	Addr2ln_offset string
	Addr2ln_name   string
	Terminate      bool
}

func A2L_resolver__init(fn string, DB_inst *sql.DB, includeInline bool) *Context {
	a, err := addr2line.New(fn)
	if err != nil {
		panic(err)
	}
	addresses := make(chan Workload, 16)
	context := &Context{a2l: a, ch_workload: addresses, DB: DB_inst}

	go workload(context, includeInline)

	return context
}

func workload(context *Context, includeInline bool) {
	var e Workload
	var counter uint64 = 0
	wg.Add(1)

	for {
		e = <-context.ch_workload
		if e.Terminate {
			wg.Done()
		} else {
			counter += 1
			context.mu.Lock()
			if counter%maxRequests == 0 {
				err := context.a2l.Close()
				if err != nil {
					panic(err)
				}
				a, err := addr2line.New("vmlinux")
				if err != nil {
					panic(err)
				}
				context.a2l = a
			}
			rs, err := context.a2l.ResolveString(e.Addr2ln_offset)
			context.mu.Unlock()

			if err == nil {
				fmt.Println(e.Addr2ln_offset, ":", rs[0].Function, "@", rs[0].File, rs[0].Line)

				if includeInline {
					for _, a := range rs[1:] {
						fmt.Println(e.Addr2ln_offset, ":", "Inlined by", a.Function, "@", a.File, a.Line)
					}
				}
			} else {
				fmt.Println(e.Addr2ln_offset, ":", e.Addr2ln_name, "Error resolving address", err.Error())
			}
		}
	}
}

func pushIntoQueue(ctx *Context, Q_WL *Workload) {
	(*ctx).ch_workload <- *Q_WL
}

func main() {
	var wl *Workload

	var dbDriver, dbDSN, instanceID string
	includeInline := false

	if len(os.Args) < 4 {
		fmt.Printf("%s <DB Driver> <DB DSN> [Include Inlines (true|false)] [Max Requests per addr2line process]", os.Args[0])
		return
	}

	dbDriver = os.Args[1]
	dbDSN = os.Args[2]
	instanceID = os.Args[3]

	if len(os.Args) >= 5 && os.Args[4] == "true" {
		includeInline = true
	}

	if len(os.Args) >= 6 {
		max, err := strconv.ParseUint(os.Args[5], 0, 64)
		if err == nil {
			maxRequests = max
		}
	}

	t := Connect_token{dbDriver, dbDSN}
	context := A2L_resolver__init("vmlinux", Connect_db(&t), includeInline)

	symbols, err := getSymbols(context, instanceID)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
	}
	//fmt.Println("Size of symbols:", len(symbols))

	file, err := os.Create("addr2line_Symbols.txt")
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
	}
	defer file.Close()
	for _, symbol := range symbols {
		_, err = file.Write([]byte(fmt.Sprintf("%s : %s\n", symbol.symbolAddress, symbol.symbolName)))
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
		}
	}
	err = file.Sync()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
	}

	for _, symbol := range symbols {
		wl = &Workload{Addr2ln_name: symbol.symbolName, Addr2ln_offset: symbol.symbolAddress, Terminate: false}
		pushIntoQueue(context, wl)
	}

	wl = &Workload{Addr2ln_name: "", Addr2ln_offset: "", Terminate: true}
	pushIntoQueue(context, wl)
	wg.Wait()
}
