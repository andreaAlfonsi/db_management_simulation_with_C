#include <stdio.h>
#include <stdlib.h>

#define bool int
#define true 1
#define false 0

typedef struct node {
	char* val;
	struct node * next;
} node_t;

typedef struct DB {
	node_t* row;
	struct DB * next;
} DB;

typedef struct nodeInd{
	int data;
	struct nodeInd *next;
} nodeInd;

typedef struct node_group_by {
	char* row;
	int count;
	struct node_group_by * next;
} node_group_by;

bool executeQuery(char* str) {
	switch (queryType(str)) {
	case 1: return create(str);
	case 2: return insert(str);
	case 3: return select(str);
	default: return false;
	}
	return true;
}

int queryType(char* str) {
	int type=0;
	char *query = strdup(str);
	query = strtok(query, " ");
	if (strcmp(query, "CREATE") == 0)
		type = 1;
	else if (strcmp(query, "INSERT") == 0)
		type = 2;
	else if (strcmp(query, "SELECT") == 0)
		type = 3;
	free(query);
	return type;
}

DB * save_ds(char * table_name) {
	FILE * ptr = fopen(strcat(table_name, ".txt"), "r");
	if (ptr == NULL) return 0;
	int line_size = 300;
	char * line = calloc(line_size, sizeof(char*));
	table_name[strlen(table_name) - 4] = 0;
	DB * DB_head = NULL;
	DB_head = calloc(1, sizeof(DB));
	if (DB_head == NULL) return 0;
	DB * DB_current = DB_head;

	//salto la prima riga
	fscanf(ptr, "%*[^\n]");
	char * a = fgetc(ptr);

	//ciclo ogni riga
	while (fgets(line, line_size, ptr) != NULL) {
		char * current_line = strdup(line);
		current_line[strlen(current_line) - 2] = 0; //rimuovo gli ultimi due caratteri della riga
		current_line = strtok(current_line, " "); 
		current_line = strtok(NULL, " "); //salto la prima parola così da ottenere gli elementi da inserire nella struttura dati

		current_line = strtok(current_line, ","); //suddivido gli elementi singolarmente
		DB_current->row = calloc(1, sizeof(node_t));;
		if (DB_current->row == NULL) return 0;
		node_t * current_DB = DB_current->row;
		while (current_line != NULL) { //inserisco gli elementi nella struttura dati
			current_DB->val = current_line;
			current_DB->next = calloc(1, sizeof(node_t));
			current_DB = current_DB->next;
			current_line = strtok(NULL, ",");
		}
		DB_current->next = calloc(1, sizeof(DB));
		DB_current = DB_current->next;
	}
	
	return DB_head;
}

DB * filter_columns(DB * db_head, char * columns1, char * table_name) {
	char * columns = strdup(columns1);
	char * words = strdup(columns);
	columns = strtok(columns, ",");
	node_t * head = malloc(sizeof(node_t));
	if (head == NULL) return 0;
	node_t * current = head;

	//ciclo ogni parola dentro columns e la inserisco nella lista
	while (columns != NULL) {
		current->val = columns;
		columns = strtok(NULL, ",");
		if (columns != NULL) {
			current->next = malloc(sizeof(node_t));
			current = current->next;
		}
		else current->next = NULL;
	}

	//leggo dal file la prima riga e salvo gli indici
	table_name = strcat(table_name, ".txt");
	FILE * ptr = fopen(table_name, "r");
	if (ptr == NULL) return 0;
	columns = (char*)malloc((17 + strlen(table_name) + strlen(words)) * sizeof(char*));
	fscanf(ptr, "%[^\n]", columns);
	fclose(ptr);
	columns += 11 + strlen(table_name);
	table_name[strlen(table_name) - 4] = 0;
	columns[strlen(columns) - 1] = 0;
	//ho in head le colonne richieste dalla query, ho in columns le colonne della tabella

	//creo una lista che conterrà solo gli indici delle colonne richieste
	current = head;
	nodeInd * q = NULL;
	q = calloc(1, sizeof(nodeInd));
	if (q == NULL) return 0;
	nodeInd * curr_q = q;
	q->data = -1;
	int cont = 0;

	//while che salva gli indici delle colonne che mi interessano
	while (current != NULL) {
		char * dup_columns = strdup(columns);
		dup_columns = strtok(dup_columns, ",");
		cont = 0;
		int changed = 0;
		while (dup_columns != NULL && current != NULL) {
			changed = 0;
			if (strcmp(current->val, dup_columns) == 0) {
				if (curr_q->data != -1) {
					curr_q->next = calloc(1, sizeof(nodeInd));
					curr_q = curr_q->next;
				}
				curr_q->data = cont;
				changed = 1;
			}
			cont++;
			if (changed == 1)
				current = current->next;
			dup_columns = strtok(NULL, ",");
		}
	}

	//creo una nuova struttura contenente solo gli elementi utili
	DB * db_current = db_head;
	DB * new_db_head = calloc(1, sizeof(DB));
	if (new_db_head == NULL) return 0;
	DB * new_db_current = new_db_head;
	new_db_current->row = calloc(1, sizeof(node_t));
	while (db_current->row != NULL) {
		curr_q = q;
		cont = 0;
		node_t * old_node=db_current->row;
		node_t * new_node = new_db_current->row;
		while (old_node != NULL && curr_q!=NULL) {
			if (curr_q->data == cont) {
				new_node->val = old_node->val;
				new_node->next = calloc(1, sizeof(node_t)); 
				if (new_node->next == NULL) return 0;
				new_node= new_node->next;
				curr_q = curr_q->next;
				cont = 0;
				old_node = db_current->row;
			}
			old_node = old_node->next;
			cont++;
		}
		db_current = db_current->next;
		new_db_current->next = calloc(1, sizeof(DB));
		new_db_current = new_db_current->next;
		new_db_current->row = calloc(1, sizeof(node_t));
	}
	return new_db_head;
}

int operator_to_use(char * operation) {
	int type = 0;
	if (strcmp(operation, "==") == 0)
		type = 1;
	else if (strcmp(operation, ">") == 0)
		type = 2;
	else if (strcmp(operation, ">=") == 0)
		type = 3;
	else if (strcmp(operation, "<") == 0)
		type = 4;
	else if (strcmp(operation, "<=") == 0)
		type = 5;
	return type;
}

char * get_columns(char * table_name) {
	FILE * ptr = fopen(strcat(table_name, ".txt"), "r");
	if (ptr == NULL) return 0;
	char * first_row = calloc(100, sizeof(char));
	fscanf(ptr, "%[^\n]", first_row);
	fclose(ptr);
	first_row += 11 + strlen(table_name);
	first_row[strlen(first_row) - 1] = 0;
	table_name[strlen(table_name) - 4] = 0;
	return first_row;
}

DB * where(DB * db_head, char * elem_1, char * elem_2, char * operation, char * table_name) {
	//trovo l'indice della colonna a cui devo fare il controllo
	int cont = 0, index=-1;
	char * columns = get_columns(strdup(table_name));
	columns = strtok(columns, ",");
	while (columns != NULL) {
		if (strcmp(columns, elem_1) == 0) {
			index = cont;
			break;
		}
		cont++;
		columns = strtok(NULL, ",");
	}

	DB * new_db = calloc(1, sizeof(DB));
	DB * current_new_db = new_db;
	DB * complete_db = save_ds(table_name);

	//ciclo tutte le righe del 'db'
	DB * db_current = db_head;
	while (db_current!=NULL && complete_db->row!=NULL) {
		if (db_current->row == NULL) break;
		node_t * current_node = complete_db->row;
		cont = 0;
		while (cont < index) {
			current_node = current_node->next;
			cont++;
		}
		int satisfied = 0;
		switch (operator_to_use(operation)) {
		case 1: if (strcmp(current_node->val, elem_2) == 0)
			satisfied = 1;
			break;
		case 2: if (strcmp(current_node->val ,elem_2)>0)
			satisfied = 1;
			break;
		case 3: if (strcmp(current_node->val , elem_2)>=0)
			satisfied = 1;
			break;
		case 4: if (strcmp(current_node->val , elem_2)<0)
			satisfied = 1;
			break;
		case 5: if (strcmp(current_node->val , elem_2)<=0)
			satisfied = 1;
			break;
		default: return 0;
			break;
	}

	//se il filtro è rispettato, aggiungo la riga corrente al nuovo db
	if (satisfied == 1) {
		current_new_db->row = db_current->row;
		current_new_db->next = calloc(1, sizeof(DB));
		current_new_db = current_new_db->next;
	}
	db_current = db_current->next;
	complete_db = complete_db->next;
	}

	return new_db;
}

node_t * get_lval(DB * head, int l, int index)
{
	while (head && l) {
		head = head->next;
		l--;
	}
	if (head != NULL) {
		int cont = 0;
		node_t * node = head->row;
		while (cont < index) {
			node = node->next;
			cont++;
		}
		return node;
	}
	else
		return -1;
}

void swap(DB * head, int i, int j) {
	DB * tmp = head;
	node_t * tmpival;
	node_t * tmpjval;
	int ti = i;
	while (tmp && i) {
		i--;
		tmp = tmp->next;
	}
	tmpival = tmp->row;
	tmp = head;
	while (tmp && j) {
		j--;
		tmp = tmp->next;
	}
	tmpjval = tmp->row;
	tmp->row = tmpival;
	tmp = head;
	i = ti;
	while (tmp && i) {
		i--;
		tmp = tmp->next;
	}
	tmp->row = tmpjval;
}

DB * quick_sort_list(DB * head, int l, int r, int index, char * order) {
	int i, j;
	char * jval;
	char * pivot;
	i = l + 1;
	if (l + 1 < r) {
		pivot = get_lval(head, l, index)->val; //ricavo il valore per il pivot
		for (j = l + 1; j < r; j++) {
			jval = get_lval(head, j, index)->val;
			if (strcmp(order, "ASC") == 0) {
				if (strcmp(jval, pivot) < 0 && jval != -1) {
					swap(head, i, j);
					i++;
				}
			}
			else if (strcmp(order, "DESC") == 0) {
				if (strcmp(jval, pivot) > 0 && jval != -1) {
					swap(head, i, j);
					i++;
				}
			}
		}
		swap(head, i - 1, l);
		quick_sort_list(head, l, i, index, order);
		quick_sort_list(head, i, r, index, order);
	}
	return head;
}

DB * order_by(DB * db_head, char * condition, char * order, char * table_name) {
	//trovo l'indice della colonna sul quale fare l'ordinamento
	int cont = 0, index = -1;
	char * columns = get_columns(strdup(table_name));
	columns = strtok(columns, ",");
	while (columns != NULL) {
		if (strcmp(columns, condition) == 0) {
			index = cont;
			break;
		}
		cont++;
		columns = strtok(NULL, ",");
	}	
	
	//conto il numero di elementi 
	DB * tmp = db_head;
	int n = 0;
	while (tmp->row) {
		n++;
		tmp = tmp->next;
	}
	//uso il quick sort
	db_head = quick_sort_list(db_head, 0, n, index, order);

	return db_head;
}

DB * group_by(DB * db_head, char * condition, char * table_name) {

	//elimino le colonne che non mi interessano
	db_head = filter_columns(db_head, condition, table_name);
	//ciclo tutto il db
	DB * tmp = db_head;
	node_group_by * new_db = calloc(1,sizeof(node_group_by));
	node_group_by * new_tmp;
	int first = 1;
	while (tmp != NULL && tmp->row->val!=NULL) {
		new_tmp = new_db;
		while (new_tmp->row != NULL && first==0){
			if (strcmp(tmp->row->val, new_tmp->row) == 0) {
				new_tmp->count++;
				break;
			}
			new_tmp = new_tmp->next;
		 } 
		if (new_tmp == NULL || new_tmp->row==NULL) {
			new_tmp->row = tmp->row->val;
			new_tmp->count++;
			new_tmp->next = calloc(1, sizeof(node_group_by));
		}
		first = 0;
		tmp = tmp->next;
	}
	return new_db;
}

void print_query_gb(node_group_by * db_head_groupby, char * query, char * table_name, char * column) {
	FILE * ptr = fopen("query_results.txt", "a");
	if (ptr == NULL) return 0;
	fprintf(ptr, "%s;\nTABLE %s COLUMNS %s,COUNT;\n", query, table_name, column);
	while (db_head_groupby->row != NULL) {
		fprintf(ptr, "ROW %s,%d;\n", db_head_groupby->row, db_head_groupby->count);
		node_group_by * tmp = db_head_groupby;
		db_head_groupby = db_head_groupby->next;
		free(tmp);
	}
	fprintf(ptr, "\n");
	fclose(ptr);
}

void print_query(DB * db_head, char * query, char * table_name, char * columns) {
	FILE * ptr = fopen("query_results.txt", "a");
	if (ptr == NULL) return 0;
	if (strcmp(columns, "*")==0) columns = get_columns(table_name);
	fprintf(ptr, "%s;\nTABLE %s COLUMNS %s;\n", query, table_name, columns);
	while (db_head!=NULL && db_head->row != NULL && db_head->row->val!=NULL) {
		fprintf(ptr, "ROW ");
		node_t * node = db_head->row;
		int first = 1;
		while (node->val != NULL) {
			if(first!=1)
				fprintf(ptr, ",%s", node->val);
			else {
				first = 0;
				fprintf(ptr, "%s", node->val);
			}
			node = node->next;
		}
		fprintf(ptr, ";\n");
		db_head = db_head->next;
	}
	fprintf(ptr, "\n");
	fclose(ptr);
}

int select(char* str) {
	char * table_name = NULL;
	char * columns = NULL;
	char * filter = NULL;

	// variabili utili in presenza di filtri nella query
	char * condition = NULL;
	char * order = NULL;
	char * elem_1 = NULL;
	char * elem_2 = NULL;
	char * operation = NULL;

	int type = 0, i = 0;

	char* query = strdup(str);
	query = strtok(query, " ");

	//ciclo tutte le parole nella query
	while (query != NULL) {
		if (i == 1) columns = strdup(query); //salvo le colonne richieste
		else if (i == 3) table_name = strdup(query); //salvo il nome della tabella
		if (i == 4) { //salvo il filtro e le variabili che mi serviranno
			filter = strdup(query);
			if (strcmp(filter, "WHERE") == 0) {
				type = 1;
				query = strtok(NULL, " ");
				elem_1 = strdup(query);
				query = strtok(NULL, " ");
				operation = strdup(query);
				query = strtok(NULL, " ");
				elem_2 = strdup(query);
			}
			else if (strcmp(query, "ORDER") == 0) {
				type = 2;
				query = strtok(NULL, " ");
				query = strtok(NULL, " ");
				condition = strdup(query);
				query = strtok(NULL, " ");
				order = strdup(query);
			}
			else if (strcmp(query, "GROUP") == 0) {
				type = 3;
				query = strtok(NULL, " ");
				query = strtok(NULL, " ");
				condition = strdup(query);
			}
		}
		i++;
		query = strtok(NULL, " ");
	}

	//inserisco tutti gli elementi della tabella richiesta in una struttura dati
	DB * db_head = save_ds(table_name);
	node_group_by * db_head_groupby=NULL;
	//gestisco filtri
	switch (type) {
	case 1: db_head = where(db_head, elem_1, elem_2, operation, table_name);
		break;
	case 2: db_head = order_by(db_head, condition, order, table_name);
		break;
	case 3: db_head_groupby = group_by(db_head, condition, table_name);
		break;
	}

	//rimuovo eventuali colonne non richieste
	if (strcmp(columns, "*") != 0 && type!=3)
		db_head=filter_columns(db_head, columns, table_name);	

	//stampo il risultato della query nel file
	if (type != 3)
		print_query(db_head, str, table_name, columns);
	else
		print_query_gb(db_head_groupby, str, table_name, condition);

	return true;
}

int create(char* str) {
	int i = 0;
	char *tableName = NULL;
	str = strtok(str, " ");
	while (str != NULL) {
		if (i == 2) {
			tableName = strdup(str);
		}
		else if (i == 3) {
			str++;
			str[strlen(str) - 1] = 0;
			FILE* ptr;
			ptr = fopen(strcat(tableName, ".txt"), "w");
			if (ptr == NULL) return 0;
			tableName[strlen(tableName) - 4] = 0;
			fprintf(ptr, "TABLE %s COLUMNS %s;\n", tableName, str);
			fclose(ptr);

			return 1;
		}
		str = strtok(NULL, " ");
		i++;
	}
	return false;
}

int insert(char* str) {
	int i = 0, j = 0;
	char *tableName = NULL;
	char *words = strdup(str);
	char *columns = NULL;
	FILE*ptr;
	words = strtok(words, " ");
	while (words != NULL) {
		if (i == 2) tableName = strdup(words);
		else if (i == 3) {
			char* tmp = strdup(words);
			tmp++;
			tmp[strlen(tmp) - 1] = 0;
			ptr = fopen(strcat(tableName, ".txt"), "r");
			if (ptr == NULL) return 0;
			columns = (char*)malloc((17 + strlen(tableName) + strlen(tmp)) * sizeof(char*));
			fscanf(ptr, "%[^\n]", columns);
			fclose(ptr);
			//estrarre dalla prima riga le colonne
			columns = strtok(columns, " ");
			while (columns != NULL && j<4) {
				if (j == 3) {
					columns[strlen(columns) - 1] = 0;
					if (strcmp(tmp, columns) != 0) return false;
				}
				else columns = strtok(NULL, " ");
				j++;
			}
			words = strdup(str);
			words = strtok(words, " ");
			words = strtok(NULL, " ");
			words = strtok(NULL, " ");
			words = strtok(NULL, " ");
		}
		else if (i == 5) {
			words++;
			words[strlen(words) - 1] = 0;
			j = 0;
			char* row = (char*)calloc((6 + strlen(words)), sizeof(char*));
			strcat(row, "ROW ");
			strcat(row, words);
			strcat(row, ";\n");
			//conto il numero di values nella query
			words = strtok(words, ",");
			while (words != NULL) {
				words = strtok(NULL, ",");
				j++;
			}
			columns = strtok(columns, ",");
			i = 0;
			while (columns != NULL) {
				columns = strtok(NULL, ",");
				i++;
			}
			//se è giusto inserisco in coda al file
			if (i == j) {
				ptr = fopen(tableName, "a");
				if (ptr == NULL) return 0;
				fprintf(ptr, row);
				fclose(ptr);
				return true;
			}
			else return false;
		}
		words = strtok(NULL, " ");
		i++;
	}
	return true;
}
