
package main

import (
	"flag"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	filosofos = 5
	ciclos    = 20
	tempoPensar = 2 * time.Millisecond
	tempoComer  = 2 * time.Millisecond
)

type Garfo struct {
	mu sync.Mutex
}

type Resultado struct {
	ciclosFeitos [filosofos]int

	mu          sync.Mutex
	esperaTotal time.Duration
	maiorEspera time.Duration
	numEsperas  int

	comendoAgora int32
	maxComendo   int32
}

func pensar() {
	time.Sleep(tempoPensar)
}

func comer() {
	time.Sleep(tempoComer)
}

func registrarEspera(r *Resultado, inicio time.Time) {
	d := time.Since(inicio)

	r.mu.Lock()
	r.esperaTotal += d
	r.numEsperas++

	if d > r.maiorEspera {
		r.maiorEspera = d
	}
	r.mu.Unlock()
}

func entrouParaComer(r *Resultado) {
	atual := atomic.AddInt32(&r.comendoAgora, 1)

	for {
		maximo := atomic.LoadInt32(&r.maxComendo)
		if atual <= maximo {
			break
		}
		if atomic.CompareAndSwapInt32(&r.maxComendo, maximo, atual) {
			break
		}
	}
}

func saiuDeParaComer(r *Resultado) {
	atomic.AddInt32(&r.comendoAgora, -1)
}

func imprimirResultado(nome string, inicio time.Time, r *Resultado) {
	fmt.Println("\nVersao:", nome)
	fmt.Println("Tempo total:", time.Since(inicio))
	fmt.Println("Maior espera:", r.maiorEspera)

	if r.numEsperas > 0 {
		fmt.Println("Espera media:", r.esperaTotal/time.Duration(r.numEsperas))
	} else {
		fmt.Println("Espera media: 0s (sem espera medida)")
	}

	fmt.Println("Maximo comendo simultaneamente:",
		atomic.LoadInt32(&r.maxComendo))

	total := 0
	for i, n := range r.ciclosFeitos {
		fmt.Printf("Filosofo %d: %d ciclos\n", i+1, n)
		total += n
	}
	fmt.Println("Total de ciclos:", total)
}

func executarV0() {
	var r Resultado
	inicio := time.Now()

	// Os filosofos executam um depois do outro.
	for i := 0; i < filosofos; i++ {
		for c := 0; c < ciclos; c++ {
			pensar()
			entrouParaComer(&r)
			comer()
			saiuDeParaComer(&r)
			r.ciclosFeitos[i]++
		}
	}

	imprimirResultado("V0 - Sequencial", inicio, &r)
}

func executarV1() {
	var garfos [filosofos]Garfo
	var wg sync.WaitGroup

	// A barreira garante que todos segurem um garfo
	// antes de tentarem pegar o segundo.
	prontos := make(chan struct{}, filosofos)
	liberar := make(chan struct{})

	for i := 0; i < filosofos; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			esquerda := id
			direita := (id + 1) % filosofos

			for c := 0; c < ciclos; c++ {
				pensar()

				garfos[esquerda].mu.Lock()
				prontos <- struct{}{}
				<-liberar

				// Todos seguram a esquerda e esperam
				// pela direita: ocorre espera circular.
				garfos[direita].mu.Lock()

				comer()

				garfos[direita].mu.Unlock()
				garfos[esquerda].mu.Unlock()
			}
		}(i)
	}

	for i := 0; i < filosofos; i++ {
		<-prontos
	}

	fmt.Println("Todos seguram um garfo.")
	fmt.Println("Liberando a tentativa de pegar o segundo...")
	close(liberar)

	// Esta espera fica bloqueada por causa do deadlock.
	wg.Wait()
}

func executarV2() {
	var garfos [filosofos]Garfo
	var r Resultado
	var wg sync.WaitGroup

	// A sala permite apenas um filosofo de cada vez.
	sala := make(chan struct{}, 1)
	inicio := time.Now()

	for i := 0; i < filosofos; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			esquerda := id
			direita := (id + 1) % filosofos

			for c := 0; c < ciclos; c++ {
				pensar()
				esperaInicio := time.Now()

				sala <- struct{}{}

				garfos[esquerda].mu.Lock()
				garfos[direita].mu.Lock()
				registrarEspera(&r, esperaInicio)

				entrouParaComer(&r)
				comer()
				saiuDeParaComer(&r)

				garfos[direita].mu.Unlock()
				garfos[esquerda].mu.Unlock()

				<-sala
				r.ciclosFeitos[id]++
			}
		}(i)
	}

	wg.Wait()
	imprimirResultado("V2 - Sincronizacao excessiva", inicio, &r)
}

func executarV3() {
	var garfos [filosofos]Garfo
	var r Resultado
	var wg sync.WaitGroup
	inicio := time.Now()

	for i := 0; i < filosofos; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			esquerda := id
			direita := (id + 1) % filosofos

			for c := 0; c < ciclos; c++ {
				pensar()

				// Sempre adquire primeiro o garfo de
				// menor indice para evitar espera circular.
				primeiro := esquerda
				segundo := direita

				if primeiro > segundo {
					primeiro, segundo = segundo, primeiro
				}

				esperaInicio := time.Now()
				garfos[primeiro].mu.Lock()
				garfos[segundo].mu.Lock()
				registrarEspera(&r, esperaInicio)

				entrouParaComer(&r)
				comer()
				saiuDeParaComer(&r)

				garfos[segundo].mu.Unlock()
				garfos[primeiro].mu.Unlock()

				r.ciclosFeitos[id]++
			}
		}(i)
	}

	wg.Wait()
	imprimirResultado("V3 - Ordenacao dos garfos", inicio, &r)
}

func main() {
	versao := flag.String("v", "v0", "v0, v1, v2 ou v3")
	flag.Parse()

	switch *versao {
	case "v0":
		executarV0()
	case "v1":
		executarV1()
	case "v2":
		executarV2()
	case "v3":
		executarV3()
	default:
		fmt.Println("Versao invalida. Use -v v0, v1, v2 ou v3.")
	}
}
