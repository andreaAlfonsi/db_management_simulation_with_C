#define bool int
#define true 1
#define false 0

/*
Struttura dati usata per rappresentare il valore di un attributo di un elemento del database.
Comprende un attributo 'val' che corrisponde al valore dell'elemento e un attributo 'next' che punta al successivo elemento della lista 
*/
typedef struct node {
	char* val;
	struct node * next;
} node_t;

/*
Struttura dati che rappresenta una 'riga' del database.
In particolare ha un attributo 'row' che punta al primo elemento della riga e un attributo 'next' che punta alla riga successiva
*/
typedef struct DB {
	node_t* row;
	struct DB * next;
} DB;

/*
Semplice lista con una variabile 'data' di tipo intero e un puntatore all'elemento successivo
*/
typedef struct nodeInd {
	int data;
	struct nodeInd *next;
} nodeInd;

/*
Particolare tipo di struttura dati usata per rappresenta il risultato di una query che utilizza il group by.
Contiene una variabile 'row' contenente il nome della riga, una variabile 'count' che indica il numero di volte che l'elemento row è presente nel database
e un puntatore all'elemento successivo
*/
typedef struct node_group_by {
	char* row;
	int count;
	struct node_group_by * next;
} node_group_by;

/*
Input:
-La stringa contenente la query da eseguire, ad esempio "CREATE TABLE studenti (matricola, nome, cognome)"
Output:
-bool: true/false, se l'esecuzione della query e' andata a buon fine o meno (presenza di eventuali errori)
*/
bool executeQuery(char*);

/*
Input:
-La stringa contenente la query da eseguire
Output:
-int: il tipo di query da eseguire (1 per CREATE, 2 per INSERT, 3 per SELECT o 0 altrimenti)
*/
int queryType(char* str);

/*
Input: 
-La stringa contenente il nome della tabella su cui lavorare
Output:
-DB*: la struttura dati contenente tutte le righe e gli elementi della tabella selezionata
*/
DB * save_ds(char * table_name);

/*
Input:
-Il database iniziale
-Le colonne da filtrare
-Il nome della tabella
Output:
DB*: il database filtrato
*/
DB * filter_columns(DB * db_head, char * columns1, char * table_name);

/*
Input:
-Stringa contenente l'operazione da eseguire
Output:
int: un numero da 1 a 5 che corrisponde all'operazione
*/
int operator_to_use(char * operation);

/*
Input:
-Stringa contenente il nome della tabella
Output:
Stringa contenente le colonne di tale tabella
*/
char * get_columns(char * table_name);

/*
Input:
-Tabella su cui lavorare
-Nome della colonna su cui effettuare il controllo
-Valore della colonna da usare nel controllo
-Operazione da controllare (==,<,<=,>,>=)
-Nome della tabella
Output:
Tabella filtrata
*/
DB * where(DB * db_head, char * elem_1, char * elem_2, char * operation, char * table_name);

/*
Input:
-La tabella
-L'indice della riga su cui lavorare
-L'indice della colonna su cui lavorare
Output:
Il nodo richiesto
*/
node_t * get_lval(DB * head, int l, int index);

/*
Input:
-La tabella
-L'indice della prima riga da scambiare
-L'indice della seconda riga da scambiare
Inverte le posizioni delle 2 righe
*/
void swap(DB * head, int i, int j);

/*
Input:
-La tabella
-Il valore di sinistra
-Il valore di destra
-L'indice della colonna sulla base della quale effettuare l'ordinamento
-Stringa che dice se l'ordinamento deve essere crescente o decrescente
*/
DB * quick_sort_list(DB * head, int l, int r, int index, char * order);

/*
Input:
-La tabella
-La colonna sulla base della quale fare l'ordinamento
-L'ordine (crescente o decrescente)
-Il nome della tabella
Output:
La tabella ordinata
*/
DB * order_by(DB * db_head, char * condition, char * order, char * table_name);

/*
Input:
-La tabella
-La colonna sulla base della quale effettuare il raggruppamento
-Il nome della tabella
Output:
La tabella aggiornata
*/
DB * group_by(DB * db_head, char * condition, char * table_name);

/*
Input:
-La tabella risultante dall'operazione di group by
-La query
-Il nome della tabella
-La colonna sulla quale è stato effettuato il group by
Stampa nel file 'query_results' il risultato della query
*/
void print_query_gb(node_group_by * db_head_groupby, char * query, char * table_name, char * column);

/*
Input:
-La tabella
-La query
-Il nome della tabella
-Le colonne richieste dalla query
Stampa nel file 'query_results' il risultato della query
*/
void print_query(DB * db_head, char * query, char * table_name, char * columns);

/*
Input:
-La query
Output:
1 se la query è riuscita
*/
int select(char* str);

/*
Input:
-La query
Output:
1 se la query è riuscita, 0 altrimenti
*/
int create(char* str);

/*
Input:
-La query
Output:
1 se la query è riuscita, 0 altrimenti
*/
int insert(char* str);