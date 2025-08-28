package evaluator

import (
	"context"
	"encoding/json"
	"fmt"
	"hael/lexer"
	"hael/object"
	"hael/parser"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"gopkg.in/zeromq/goczmq.v4"
	// "syscall/js"
)

var client = &http.Client{
	Timeout: 3 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:       100,
		IdleConnTimeout:    30 * time.Second,
		DisableKeepAlives:  false,
		DisableCompression: true,
	},
}

var (
	queues   = make(map[string]chan object.Object)
	queuesMu sync.Mutex
)

func getQueue(alias string) chan object.Object {
	queuesMu.Lock()
	defer queuesMu.Unlock()

	q, ok := queues[alias]
	if !ok {
		q = make(chan object.Object, 100000)
		queues[alias] = q
	}
	return q
}

var publishers = make(map[string]*goczmq.Sock)

var subscribers = make(map[string]*goczmq.Sock)

func ZmqPub(addr, topic, msg string) object.Object {
	pub, ok := publishers[addr]
	if !ok {
		var err error
		pub, err = goczmq.NewPub(addr)
		if err != nil {
			return newError("erro criando publisher: %w")
		}
		publishers[addr] = pub
	}

	err := pub.SendFrame([]byte(fmt.Sprintf("%s %s", topic, msg)), goczmq.FlagNone)
	return newError("erro criando publisher: ", err)
}

func ZmqSub(addr, topic string, handler func(msg string)) error {
	sub, ok := subscribers[addr]
	if !ok {
		var err error
		sub, err = goczmq.NewSub(addr, topic)
		if err != nil {
			return fmt.Errorf("erro criando subscriber: %w", err)
		}
		subscribers[addr] = sub
	}

	go func() {
		for {
			frames, _, err := sub.RecvFrame()
			if err != nil {
				log.Printf("erro recebendo frame: %v", err)
				continue
			}
			if len(frames) > 0 {
				handler(string(frames))
			}
		}
	}()
	return nil
}

type ProtectedValue struct {
	mu    sync.RWMutex
	value object.Object
}

var (
	protectedStore = make(map[string]*ProtectedValue)
	storeMu        sync.RWMutex
)

func SetProtected(alias string, val object.Object) {
	storeMu.Lock()
	defer storeMu.Unlock()
	pv, ok := protectedStore[alias]
	if !ok {
		pv = &ProtectedValue{}
		protectedStore[alias] = pv
	}
	pv.mu.Lock()
	defer pv.mu.Unlock()
	pv.value = val
}

func GetProtected(alias string) (object.Object, bool) {
	storeMu.RLock()
	pv, ok := protectedStore[alias]
	storeMu.RUnlock()
	if !ok {
		return nil, false
	}
	pv.mu.RLock()
	defer pv.mu.RUnlock()
	return pv.value, true
}

var (
	buffers   = make(map[string]*object.Array)
	buffersMu sync.Mutex
)

var pgPools struct {
	mu sync.RWMutex
	m  map[string]*pgxpool.Pool
}

func init() {
	pgPools.m = make(map[string]*pgxpool.Pool)
}

func pgGet(alias string) (*pgxpool.Pool, bool) {
	pgPools.mu.RLock()
	p, ok := pgPools.m[alias]
	pgPools.mu.RUnlock()
	return p, ok
}

func pgSet(alias string, pool *pgxpool.Pool) {
	pgPools.mu.Lock()
	pgPools.m[alias] = pool
	pgPools.mu.Unlock()
}

func pgDelete(alias string) {
	pgPools.mu.Lock()
	delete(pgPools.m, alias)
	pgPools.mu.Unlock()
}

var Builtins map[string]*object.Builtin

func InitBuiltins(env *object.Environment) {
	Builtins = map[string]*object.Builtin{
		"len": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 1 {
					return newError("wrong number of arguments. got=%d, want=1", len(args))
				}

				switch arg := args[0].(type) {
				case *object.Array:
					return &object.Integer{Value: int64(len(arg.Elements))}
				case *object.String:
					return &object.Integer{Value: int64(len(arg.Value))}
				default:
					return newError("argument to `len` not supported, got %s", args[0].Type())
				}
			},
		},
		"push": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 2 {
					return newError("wrong number of arguments. got=%d, want=2", len(args))
				}

				if args[0].Type() != object.ARRAY_OBJ {
					return newError("argument to `push` must be ARRAY, got %s", args[0].Type())
				}

				arr := args[0].(*object.Array)
				length := len(arr.Elements)

				newElements := make([]object.Object, length+1)
				copy(newElements, arr.Elements)
				newElements[length] = args[1]

				return &object.Array{Elements: newElements}
			},
		},
		"pop": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 1 {
					return newError("wrong number of arguments. got=%d, want=1", len(args))
				}

				if args[0].Type() != object.ARRAY_OBJ {
					return newError("argument to `pop` must be ARRAY, got %s", args[0].Type())
				}

				arr := args[0].(*object.Array)
				if len(arr.Elements) == 0 {
					return newError("cannot pop from empty array")
				}

				lastElement := arr.Elements[len(arr.Elements)-1]

				newElements := make([]object.Object, len(arr.Elements)-1)
				copy(newElements, arr.Elements[:len(arr.Elements)-1])
				arr.Elements = newElements

				return lastElement
			},
		},
		"type": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 1 {
					return newError("type expects one argument")
				}
				return &object.String{Value: string(args[0].Type())}
			},
		},
		"slice": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) < 2 || len(args) > 3 {
					return newError("wrong number of arguments. got=%d, want=2 or 3", len(args))
				}

				if args[0].Type() != object.ARRAY_OBJ {
					return newError("first argument to `slice` must be ARRAY, got %s", args[0].Type())
				}

				if args[1].Type() != object.INTEGER_OBJ {
					return newError("second argument to `slice` must be INTEGER, got %s", args[1].Type())
				}

				end := int64(len(args[0].(*object.Array).Elements))

				if len(args) == 3 {
					if args[2].Type() != object.INTEGER_OBJ {
						return newError("third argument to `slice` must be INTEGER, got %s", args[2].Type())
					}
					end = args[2].(*object.Integer).Value
				}

				arr := args[0].(*object.Array)
				start := args[1].(*object.Integer).Value

				if start < 0 || end > int64(len(arr.Elements)) || start > end {
					return newError("invalid slice indices: start=%d, end=%d", start, end)
				}

				newElements := make([]object.Object, end-start)
				copy(newElements, arr.Elements[start:end])

				return &object.Array{Elements: newElements}
			},
		},
		"print": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				for _, text := range args {
					println(text.Inspect())
				}

				return NULL
			},
		},
		"do": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				fn, ok := args[0].(*object.Function)
				if ok {
					return Eval(fn.Body, env)
				}

				return NULL
			},
		},
		"map": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 2 {
					newError("Expected exactly 2 arguments,got=%d", len(args))
					return NULL
				}

				array, ok := args[0].(*object.Array)
				if !ok {
					newError("Expected array,got=%s", args[0].Type())
					return NULL
				}

				fn, ok := args[1].(*object.Function)

				if !ok {
					newError("Expected Function, got=%s", args[1].Type())
					return NULL
				}

				if len(fn.Parameters) != 1 {
					newError("Function should have exactly 1 argument, got=%d", len(fn.Parameters))
					return NULL
				}

				functionEnv := object.NewEnclosedEnvironment(env)
				var result object.Array
				if ok {
					for _, item := range array.Elements {
						functionEnv.Set(fn.Parameters[0].String(), item)
						result.Elements = append(result.Elements, Eval(fn.Body, functionEnv))
					}
				}

				return &result
			},
		},
		"floatToInt": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 1 {
					return newError("floatToInt expects 1 argument")
				}
				strArg, ok := args[0].(*object.String)
				if !ok {
					return newError("floatToInt: argument must be STRING")
				}
				f, err := strconv.ParseFloat(strArg.Value, 64)
				if err != nil {
					return newError("floatToInt: cannot parse float: %s", err.Error())
				}
				return &object.Integer{Value: int64(f * 1000)}
			},
		},
		"intToFloat": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 1 {
					return newError("intToFloat expects 1 argument")
				}
				intArg, ok := args[0].(*object.Integer)
				if !ok {
					strArg, ok := args[0].(*object.String)
					if !ok {
						return newError("intToFloat: argument must be INTEGER OR STRING")
					}
					val, err := strconv.ParseInt(strArg.Value, 10, 64)
					if err != nil {
						return newError("intToFloat: argument must be a number")
					}
					return &object.String{Value: fmt.Sprintf("%.3f", (float64(val) / 1000))}
				}
				// Divide por 1000 e retorna como string
				return &object.String{Value: fmt.Sprintf("%.3f", float64(intArg.Value)/1000)}
			},
		},
		"fast_request": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) < 2 {
					return newError("http_request expects method, url, [body], [headers]")
				}
				method := args[0].(*object.String).Value
				url := args[1].(*object.String).Value

				var body io.Reader = nil
				if len(args) >= 3 {
					if s, ok := args[2].(*object.String); ok {
						body = strings.NewReader(s.Value)
					}
				}

				req, err := http.NewRequest(method, url, body)
				if err != nil {
					return newError("failed request: %s", err.Error())
				}

				if len(args) == 4 {
					if h, ok := args[3].(*object.Hash); ok {
						for _, pair := range h.Pairs {
							key := pair.Key.(*object.String).Value
							val := pair.Value.(*object.String).Value
							req.Header.Set(key, val)
						}
					}
				}

				resp, err := client.Do(req)
				if err != nil {
					return newError("failed request: %s", err.Error())
				}
				resp.Body.Close()

				return &object.Hash{Pairs: map[object.HashKey]object.HashPair{
					(&object.String{Value: "status"}).HashKey(): {Key: &object.String{Value: "status"}, Value: &object.Integer{Value: int64(resp.StatusCode)}},
				}}
			},
		},
		"http_request": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) < 2 {
					return newError("http_request expects method, url, [body], [headers]")
				}
				method := args[0].(*object.String).Value
				url := args[1].(*object.String).Value

				var body io.Reader = nil
				if len(args) >= 3 {
					if s, ok := args[2].(*object.String); ok {
						body = strings.NewReader(s.Value)
					}
				}

				req, err := http.NewRequest(method, url, body)
				if err != nil {
					return newError("failed request: %s", err.Error())
				}

				if len(args) == 4 {
					if h, ok := args[3].(*object.Hash); ok {
						for _, pair := range h.Pairs {
							key := pair.Key.(*object.String).Value
							val := pair.Value.(*object.String).Value
							req.Header.Set(key, val)
						}
					}
				}

				resp, err := client.Do(req)
				if err != nil {
					return newError("failed request: %s", err.Error())
				}
				defer resp.Body.Close()
				data, _ := io.ReadAll(resp.Body)

				return &object.Hash{Pairs: map[object.HashKey]object.HashPair{
					(&object.String{Value: "status"}).HashKey(): {Key: &object.String{Value: "status"}, Value: &object.Integer{Value: int64(resp.StatusCode)}},
					(&object.String{Value: "body"}).HashKey():   {Key: &object.String{Value: "body"}, Value: &object.String{Value: string(data)}},
				}}
			},
		},
		"json_parse": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 1 {
					return newError("json_parse expects 1 string")
				}
				var m map[string]interface{}
				if err := json.Unmarshal([]byte(args[0].(*object.String).Value), &m); err != nil {
					return newError("invalid json: %s", args[0])
				}
				return GoToHael(m)
			},
		},
		"getenv": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 1 {
					return newError("getenv expects 1 string")
				}
				key := args[0].(*object.String).Value
				return &object.String{Value: os.Getenv(key)}
			},
		},
		"now": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				return &object.Integer{Value: time.Now().UnixMilli()}
			},
		},
		"nowIso": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 0 {
					return newError("now expects no args")
				}

				iso := time.Now().UTC().Format(time.RFC3339)
				return &object.String{Value: iso}
			},
		},
		"Number": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				strNum := args[0].(*object.String).Value
				num, err := strconv.ParseInt(strNum, 10, 64)
				if err != nil {
					fmt.Printf("Erro ao converter a string '%s': %v\n", strNum, err)
					return NULL
				}
				return &object.Integer{Value: num}
			},
		},
		"json_stringify": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 1 {
					return newError("json_stringify expects 1 arg")
				}
				bytes, err := json.Marshal(HaelToGo(args[0]))
				if err != nil {
					return newError("cannot stringify: %s", err.Error())
				}
				return &object.String{Value: string(bytes)}
			},
		},
		"http_listen": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 2 {
					return newError("wrong number of arguments. got=%d, want=2", len(args))
				}

				portObj, ok := args[0].(*object.String)
				if !ok {
					return newError("1st argument to `http_listen` must be STRING, got %s", args[0].Type())
				}

				handlerFn, ok := args[1].(*object.Function)
				if !ok {
					return newError("2nd argument to `http_listen` must be FUNCTION, got %s", args[1].Type())
				}

				mux := http.NewServeMux()

				mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
					queryHash := &object.Hash{Pairs: make(map[object.HashKey]object.HashPair)}
					for key, vals := range r.URL.Query() {
						if len(vals) > 0 {
							strKey := &object.String{Value: key}
							strVal := &object.String{Value: vals[0]}
							queryHash.Pairs[strKey.HashKey()] = object.HashPair{
								Key:   strKey,
								Value: strVal,
							}
						}
					}

					req := &object.Hash{Pairs: map[object.HashKey]object.HashPair{
						(&object.String{Value: "path"}).HashKey(): {
							Key:   &object.String{Value: "path"},
							Value: &object.String{Value: r.URL.Path},
						},
						(&object.String{Value: "method"}).HashKey(): {
							Key:   &object.String{Value: "method"},
							Value: &object.String{Value: r.Method},
						},
						(&object.String{Value: "query"}).HashKey(): {
							Key:   &object.String{Value: "query"},
							Value: queryHash,
						},
						(&object.String{Value: "body"}).HashKey(): {
							Key: &object.String{Value: "body"},
							Value: &object.String{Value: func() string {
								b, _ := io.ReadAll(r.Body)
								if string(b) == "" {
									return "{}"
								}
								return string(b)
							}()},
						},
					}}

					functionEnv := object.NewEnclosedEnvironment(env)
					functionEnv.Set("req", req)

					result := Eval(handlerFn.Body, functionEnv)

					if result.Type() == object.ERROR_OBJ {
						http.Error(w, result.Inspect(), http.StatusInternalServerError)
						return
					}

					fmt.Fprintf(w, "%s", result.Inspect())
				})

				http.ListenAndServe(":"+portObj.Value, mux)
				return GOOD
			},
		},
		"import": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 1 {
					return newError("wrong number of arguments. got=%d, want=1", len(args))
				}

				pathObj, ok := args[0].(*object.String)
				if !ok {
					return newError("argument to `import` must be STRING, got %s", args[0].Type())
				}

				callerFileObj, _ := env.Get("__current_file__")
				callerDir := ""
				if callerFileObj != nil && callerFileObj.Type() == object.STRING_OBJ {
					callerDir = filepath.Dir(callerFileObj.(*object.String).Value)
				}

				// Resolve o path absoluto do arquivo a ser importado
				absImportPath := filepath.Join(callerDir, pathObj.Value)
				absImportPath, err := filepath.Abs(absImportPath)
				if err != nil {
					return newError("failed to resolve absolute path: %v", err)
				}

				content, err := os.ReadFile(absImportPath)
				if err != nil {
					return newError("failed to read file %s: %v", absImportPath, err)
				}

				// Faz parse do arquivo
				l := lexer.New(string(content))
				p := parser.New(l)
				program := p.ParseProgram()
				if len(p.Errors()) != 0 {
					return newError("parse errors in %s: %v", absImportPath, p.Errors())
				}

				// Guarda o __current_file__ atual para restaurar depois
				prevFileObj, _ := env.Get("__current_file__")
				env.Set("__current_file__", &object.String{Value: absImportPath})

				// Avalia o programa no mesmo ambiente
				Eval(program, env)

				// Restaura __current_file__ anterior
				env.Set("__current_file__", prevFileObj)

				return GOOD
			},
		},
		"parseHTML": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 1 {
					return newError("wrong number of arguments. got=%d, want=1", len(args))
				}

				path, ok := args[0].(*object.String)
				if !ok {
					return newError("argument to `parseHTML` must be STRING, got %s", args[0].Type())
				}

				content, err := os.ReadFile(path.Value)
				if err != nil {
					return newError("failed to read file %s: %v", path.Value, err)
				}

				return &object.String{Value: string(content)}
			},
		},
		"pg_connect": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 2 {
					return newError("pg_connect expects 2 args: dsn, alias")
				}
				dsnObj, ok1 := args[0].(*object.String)
				aliasObj, ok2 := args[1].(*object.String)
				if !ok1 || !ok2 {
					return newError("pg_connect: both dsn and alias must be STRING")
				}
				dsn := dsnObj.Value
				alias := aliasObj.Value

				cfg, err := pgxpool.ParseConfig(dsn)
				if err != nil {
					return newError("pg_connect: invalid dsn: %s", err.Error())
				}

				cfg.MaxConns = 25
				cfg.MinConns = 5
				cfg.HealthCheckPeriod = 30 * time.Second
				cfg.MaxConnLifetime = time.Minute * 5
				cfg.MaxConnIdleTime = time.Minute

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				pool, err := pgxpool.NewWithConfig(ctx, cfg)
				if err != nil {
					return newError("pg_connect failed: %s", err.Error())
				}

				pgSet(alias, pool)
				return GOOD
			},
		},
		"pg_close": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				// pg_close(alias)
				if len(args) != 1 {
					return newError("pg_close expects 1 arg: alias")
				}
				aliasObj, ok := args[0].(*object.String)
				if !ok {
					return newError("pg_close: alias must be STRING")
				}
				alias := aliasObj.Value
				if pool, ok := pgGet(alias); ok {
					pool.Close()
					pgDelete(alias)
					return GOOD
				}
				return newError("pg_close: alias not found")
			},
		},
		"async": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				go func() {
					// if len(args) != 1 {
					// 	// return newError("async expects 1 arg: function")
					// }

					fn, _ := args[0].(*object.Function)
					// if !ok {
					// 	// return newError("async: arg must be FUNCTION")
					// }

					fnEnv := object.NewEnclosedEnvironment(env)
					_ = Eval(fn.Body, fnEnv)
				}()

				return GOOD
			},
		},
		"pg_query": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				// pg_query(alias, sql, [paramsArray])
				if len(args) < 2 || len(args) > 3 {
					return newError("pg_query expects alias, sql, [params]")
				}
				aliasObj, ok1 := args[0].(*object.String)
				sqlObj, ok2 := args[1].(*object.String)
				if !ok1 || !ok2 {
					return newError("pg_query: alias and sql must be STRING")
				}
				pool, ok := pgGet(aliasObj.Value)
				if !ok {
					return newError("pg_query: alias not connected")
				}

				var params []any
				var err error
				if len(args) == 3 {
					params, err = haelArrayToArgs(args[2])
					if err != nil {
						return newError("pg_query params: %s", err.Error())
					}
				}

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				rows, err := pool.Query(ctx, sqlObj.Value, params...)
				if err != nil {
					return newError("pg_query failed: %s", err.Error())
				}
				defer rows.Close()

				// nomes de colunas
				fds := rows.FieldDescriptions()
				cols := make([]string, len(fds))
				for i, fd := range fds {
					cols[i] = string(fd.Name)
				}

				// coleta linhas
				out := &object.Array{Elements: []object.Object{}}
				for rows.Next() {
					vals := make([]any, len(cols))
					ptrs := make([]any, len(cols))
					for i := range vals {
						ptrs[i] = &vals[i]
					}
					if err := rows.Scan(ptrs...); err != nil {
						return newError("pg_query scan: %s", err.Error())
					}
					out.Elements = append(out.Elements, rowToHael(cols, vals))
				}
				if err := rows.Err(); err != nil {
					return newError("pg_query rows: %s", err.Error())
				}
				return out
			},
		},
		"pg_exec": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				// pg_exec(alias, sql, [paramsArray]) -> { rowsAffected: int }
				if len(args) < 2 || len(args) > 3 {
					return newError("pg_exec expects alias, sql, [params]")
				}
				aliasObj, ok1 := args[0].(*object.String)
				sqlObj, ok2 := args[1].(*object.String)
				if !ok1 || !ok2 {
					return newError("pg_exec: alias and sql must be STRING")
				}
				pool, ok := pgGet(aliasObj.Value)
				if !ok {
					return newError("pg_exec: alias not connected")
				}

				var params []any
				var err error
				if len(args) == 3 {
					params, err = haelArrayToArgs(args[2])
					if err != nil {
						return newError("pg_exec params: %s", err.Error())
					}
				}

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()

				tag, err := pool.Exec(ctx, sqlObj.Value, params...)
				if err != nil {
					return newError("pg_exec failed: %s", err.Error())
				}

				result := &object.Hash{Pairs: map[object.HashKey]object.HashPair{}}
				k := &object.String{Value: "rowsAffected"}
				v := &object.Integer{Value: int64(tag.RowsAffected())}
				result.Pairs[k.HashKey()] = object.HashPair{Key: k, Value: v}
				return result
			},
		},
		"buffer_new": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 1 {
					return newError("buffer_new expects 1 arg: name")
				}
				name, ok := args[0].(*object.String)
				if !ok {
					return newError("buffer_new: name must be STRING")
				}

				buffersMu.Lock()
				defer buffersMu.Unlock()
				if _, ok := buffers[name.Value]; ok {
					return newError("buffer_new: already exists")
				}
				buffers[name.Value] = &object.Array{Elements: []object.Object{}}
				return GOOD
			},
		},
		"buffer_add": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 2 {
					return newError("buffer_add expects 2 args: name, item")
				}
				name, ok := args[0].(*object.String)
				if !ok {
					return newError("buffer_add: name must be STRING")
				}

				buffersMu.Lock()
				defer buffersMu.Unlock()
				if buf, ok := buffers[name.Value]; ok {
					buf.Elements = append(buf.Elements, args[1])
					return &object.Integer{Value: int64(len(buf.Elements))}
				}
				return newError("buffer_add: buffer not found")
			},
		},
		"buffer_len": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 1 {
					return newError("buffer_add expects 1 args: name")
				}
				name, ok := args[0].(*object.String)
				if !ok {
					return newError("buffer_add: name must be STRING")
				}

				if buf, ok := buffers[name.Value]; ok {
					return &object.Integer{Value: int64(len(buf.Elements))}
				}
				return newError("buffer_len: buffer not found")
			},
		},
		"sleep": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 1 {
					return newError("sleep expects 1 arg: duration")
				}
				sleep, ok := args[0].(*object.Integer)
				if !ok {
					return newError("sleep: duration must be INTEGER")
				}
				time.Sleep(time.Duration(sleep.Value) * time.Millisecond)
				return NULL
			},
		},
		"buffer_flush": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 1 {
					return newError("buffer_flush expects 1 arg: name")
				}
				name, ok := args[0].(*object.String)
				if !ok {
					return newError("buffer_flush: name must be STRING")
				}

				buffersMu.Lock()
				defer buffersMu.Unlock()
				if buf, ok := buffers[name.Value]; ok {
					elems := buf.Elements
					buf.Elements = []object.Object{}
					return &object.Array{Elements: elems}
				}
				return newError("buffer_flush: buffer not found")
			},
		},
		"lock_put": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 2 {
					return newError("store(alias, value)")
				}
				alias, ok := args[0].(*object.String)
				if !ok {
					return newError("alias deve ser string")
				}
				SetProtected(alias.Value, args[1])
				return NULL
			},
		},
		"lock_get": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 1 {
					return newError("load(alias)")
				}
				alias, ok := args[0].(*object.String)
				if !ok {
					return newError("alias deve ser string")
				}
				val, ok := GetProtected(alias.Value)
				if !ok {
					return NULL
				}
				return val
			},
		},
		"mq_pub": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 3 {
					return newError("zmq_pub espera 3 argumentos (addr, topic, msg)")
				}

				addr, ok1 := args[0].(*object.String)
				topic, ok2 := args[1].(*object.String)
				msg, ok3 := args[2].(*object.String)

				if !ok1 || !ok2 || !ok3 {
					return newError("argumentos inválidos para zmq_pub (precisa ser string, string, string)")
				}

				err := ZmqPub(addr.Value, topic.Value, msg.Value)
				if err != nil {
					return newError(fmt.Sprintf("erro no zmq_pub: %s", err))
				}

				return NULL
			},
		},
		"mq_sub": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 3 {
					return newError("zmq_sub espera 3 argumentos (addr, topic, handlerFn)")
				}

				addr, ok1 := args[0].(*object.String)
				topic, ok2 := args[1].(*object.String)
				handler, ok3 := args[2].(*object.Function)

				if !ok1 || !ok2 || !ok3 {
					return newError("argumentos inválidos para zmq_sub (precisa ser string, string, fn)")
				}

				err := ZmqSub(addr.Value, topic.Value, func(msg string) {
					parts := strings.SplitN(msg, " ", 2)
					msg = parts[1]
					fnEnv := object.NewEnclosedEnvironment(env)
					fnEnv.Set(handler.Parameters[0].Value, &object.String{Value: msg})
					Eval(handler.Body, fnEnv)
				})
				if err != nil {
					return newError(fmt.Sprintf("erro no zmq_sub: %s", err))
				}

				return NULL
			},
		},
		"enqueue": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 2 {
					return newError("queue_enqueue expects alias, object")
				}
				alias, ok := args[0].(*object.String)
				if !ok {
					return newError("queue_enqueue alias must be string")
				}
				q := getQueue(alias.Value)
				q <- args[1]
				return NULL
			},
		},
		"dequeue": &object.Builtin{
			Fn: func(args ...object.Object) object.Object {
				if len(args) != 1 {
					return newError("queue_dequeue expects alias")
				}
				alias, ok := args[0].(*object.String)
				if !ok {
					return newError("queue_dequeue alias must be string")
				}
				q := getQueue(alias.Value)
				item := <-q
				return item
			},
		},
	}
}

func GoToHael(val interface{}) object.Object {
	switch v := val.(type) {
	case string:
		return &object.String{Value: v}
	case float64:
		return &object.String{Value: fmt.Sprint(float64(v))}
	case bool:
		if v {
			return &object.Boolean{Value: true}
		}
		return &object.Boolean{Value: false}
	case nil:
		return &object.Null{}
	case map[string]interface{}:
		pairs := make(map[object.HashKey]object.HashPair)
		for key, value := range v {
			k := &object.String{Value: key}
			hv := GoToHael(value)
			pairs[k.HashKey()] = object.HashPair{Key: k, Value: hv}
		}
		return &object.Hash{Pairs: pairs}
	case []interface{}:
		elems := []object.Object{}
		for _, e := range v {
			elems = append(elems, GoToHael(e))
		}
		return &object.Array{Elements: elems}
	default:
		return &object.String{Value: fmt.Sprintf("%v", v)}
	}
}
func HaelToGo(obj object.Object) interface{} {
	switch v := obj.(type) {
	case *object.String:
		if i, err := strconv.ParseInt(v.Value, 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(v.Value, 64); err == nil {
			return f
		}

		return v.Value
	case *object.Integer:
		return v.Value
	case *object.Boolean:
		return v.Value
	case *object.Null:
		return nil
	case *object.Hash:
		m := map[string]interface{}{}
		for _, pair := range v.Pairs {
			if keyStr, ok := pair.Key.(*object.String); ok {
				m[keyStr.Value] = HaelToGo(pair.Value)
			}
		}
		return m
	case *object.Array:
		arr := []interface{}{}
		for _, e := range v.Elements {
			arr = append(arr, HaelToGo(e))
		}
		return arr
	default:
		return fmt.Sprintf("%v", obj.Inspect())
	}
}

func haelArrayToArgs(o object.Object) ([]any, error) {
	if o == nil || o.Type() == object.NULL_OBJ {
		return nil, nil
	}
	arr, ok := o.(*object.Array)
	if !ok {
		return nil, fmt.Errorf("params must be ARRAY")
	}
	out := make([]any, 0, len(arr.Elements))
	for _, el := range arr.Elements {
		out = append(out, HaelToGo(el))
	}
	return out, nil
}

func rowToHael(cols []string, vals []any) *object.Hash {
	pairs := make(map[object.HashKey]object.HashPair, len(cols))
	for i, name := range cols {
		k := &object.String{Value: name}
		v := GoToHael(vals[i])
		pairs[k.HashKey()] = object.HashPair{Key: k, Value: v}
	}
	return &object.Hash{Pairs: pairs}
}
