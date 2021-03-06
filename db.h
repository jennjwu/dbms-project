/********************************************************************
db.h - This file contains all the structures, defines, and function
	prototype for the db.exe program.
*********************************************************************/

#define MAX_IDENT_LEN   16
#define MAX_NUM_COL		16
#define MAX_TOK_LEN		32
#define KEYWORD_OFFSET	10
#define STRING_BREAK	" (),<>="
#define NUMBER_BREAK	" ),"
#define MAX_PRINT_LEN   544

/* Column descriptor sturcture = 20+4+4+4+4 = 36 bytes */
typedef struct cd_entry_def
{
	char		col_name[MAX_IDENT_LEN+4];
	int			col_id;                   /* Start from 0 */
	int			col_type;
	int			col_len;
	int 		not_null;
} cd_entry;

/* Table packed descriptor sturcture = 4+20+4+4+4 = 36 bytes
   Minimum of 1 column in a table - therefore minimum size of
	 1 valid tpd_entry is 36+36 = 72 bytes. */
typedef struct tpd_entry_def
{
	int				tpd_size;
	char			table_name[MAX_IDENT_LEN+4];
	int				num_columns;
	int				cd_offset;
	int				tpd_flags;
} tpd_entry;

/* Table packed descriptor list = 4+4+4+36 = 48 bytes.  When no
   table is defined the tpd_list is 48 bytes.  When there is 
	 at least 1 table, then the tpd_entry (36 bytes) will be
	 overlapped by the first valid tpd_entry. */
typedef struct tpd_list_def
{
	int				list_size;
	int				num_tables;
	int				db_flags;
	tpd_entry		tpd_start;
}tpd_list;

/* This token_list definition is used for breaking the command
   string into separate tokens in function get_tokens().  For
	 each token, a new token_list will be allocated and linked 
	 together. */
typedef struct t_list
{
	char	tok_string[MAX_TOK_LEN];
	int		tok_class;
	int		tok_value;
	struct t_list *next;
} token_list;

/* Table file header definition used to store data inserted into
   a table.
*/
typedef struct table_file_header_def
{
	int			file_size;				// 4 bytes
	int			record_size;			// 4 bytes
	int			num_records;			// 4 bytes
	int			record_offset;			// 4 bytes
	int			file_header_flag;		// 4 bytes
	tpd_entry	*tpd_ptr;				// 4 bytes
} table_file_header;					// minimum size = 24

/* This enum defines the different classes of tokens for 
	 semantic processing. */
typedef enum t_class
{
	keyword = 1,	// 1
	identifier,		// 2
	symbol, 		// 3 
	type_name,		// 4
	constant,		// 5
	function_name,	// 6
	terminator,		// 7
	error			// 8
} token_class;

/* This enum defines the different values associated with
   a single valid token.  Use for semantic processing. */
typedef enum t_value
{
	T_INT = 10,		// 10 - new type should be added above this line
	T_CHAR,		    // 11 
	K_CREATE, 		// 12
	K_TABLE,		// 13
	K_NOT,			// 14
	K_NULL,			// 15
	K_DROP,			// 16
	K_LIST,			// 17
	K_SCHEMA,		// 18
	K_FOR,			// 19
	K_TO,			// 20
	K_INSERT,		// 21
	K_INTO,       // 22
	K_VALUES,     // 23
	K_DELETE,     // 24
	K_FROM,       // 25
	K_WHERE,      // 26
	K_UPDATE,     // 27
	K_SET,        // 28
	K_SELECT,     // 29
	K_ORDER,      // 30
	K_BY,         // 31
	K_DESC,       // 32
	K_IS,         // 33
	K_AND,        // 34
	K_OR,         // 35 - new keyword should be added below this line
	K_BACKUP,	  // 36
	K_RESTORE, 	  // 37
	K_ROLLFORWARD,	// 38
	K_WITHOUT,		// 39
	K_RF,			// 40
	F_SUM,        // 41
	F_AVG,        // 42
	F_COUNT,      // 43 - new function name should be added below this line
	S_LEFT_PAREN = 70,  // 70
	S_RIGHT_PAREN,		// 71
	S_COMMA,			// 72
	S_STAR,             // 73
	S_EQUAL,            // 74
	S_LESS,             // 75
	S_GREATER,          // 76
	IDENT = 85,			// 85 - for identifiers
	INT_LITERAL = 90,	// 90 - for constants
	STRING_LITERAL,     // 91
	EOC = 95,		    // 95 - terminator
	INVALID = 99		// 99 - invalid tokens (class = token, value = invalid)
} token_value;

/* This constants must be updated when add new keywords */
#define TOTAL_KEYWORDS_PLUS_TYPE_NAMES 34

/* New keyword must be added in the same position/order as the enum
   definition above, otherwise the lookup will be wrong */
char *keyword_table[] = 
{
  "int", "char", "create", "table", "not", "null", "drop", "list", "schema",
  "for", "to", "insert", "into", "values", "delete", "from", "where", 
  "update", "set", "select", "order", "by", "desc", "is", "and", "or",
  "backup", "restore", "rollforward", "without", "rf", "sum", "avg", "count"
};

/* This enum defines a set of possible statements */
typedef enum s_statement
{
  INVALID_STATEMENT = -199,	// -199
  CREATE_TABLE = 100,		// 100
  DROP_TABLE,				// 101
  LIST_TABLE,				// 102
  LIST_SCHEMA,				// 103
  INSERT,                   // 104
  DELETE,                   // 105
  UPDATE,                   // 106
  SELECT,                   // 107
  BACKUP,					// 108
  RESTORE,					// 109
  ROLLFORWARD 				// 110
} semantic_statement;

/* This enum has a list of all the errors that should be detected
   by the program.  Can append to this if necessary. */
typedef enum error_return_codes
{
	INVALID_TABLE_NAME = -399,	// -399
	DUPLICATE_TABLE_NAME,		// -398
	TABLE_NOT_EXIST,			// -397
	INVALID_TABLE_DEFINITION,	// -396
	INVALID_COLUMN_NAME,		// -395
	DUPLICATE_COLUMN_NAME,		// -394
	COLUMN_NOT_EXIST,			// -393
	MAX_COLUMN_EXCEEDED,		// -392
	INVALID_TYPE_NAME,			// -391
	INVALID_COLUMN_DEFINITION,	// -390
	INVALID_COLUMN_LENGTH,		// -389
	INVALID_REPORT_FILE_NAME,	// -388

	/* errors for insert */
	INVALID_INSERT_STATEMENT = -380,	// -380
	INSERT_VALUE_TYPE_MISMATCH,			// -379
	INSERT_CHAR_LENGTH_MISMATCH,		// -378
	INSERT_VAL_COL_LEN_MISMATCH,		// -377
	INSERT_NULL_FAILURE,				// -376
	INSERT_INT_SIZE_FAILURE,			// -375
	MAX_NUM_RECORDS,					// -374
	INSERT_INVALID_TYPE,				// -373
	
	/* errors for delete */
	INVALID_DELETE_STATEMENT = -365,		// -365
	INVALID_WHERE_CLAUSE_IN_DELETE,			// -364
	INVALID_REL_COMP_IN_WHERE_IN_DELETE,	// -363
	INVALID_TYPE_IN_WHERE_OF_DELETE,		// -362
	MISMATCH_TYPE_IN_WHERE_OF_DELETE,		// -361
	NO_MATCHING_ROWS_TO_DELETE,				// -360
	
	/* errors for update */
	INVALID_UPDATE_STATEMENT = -350, 		// -350
	NO_MATCHING_COL_TO_UPDATE,				// -349
	INVALID_REL_OP_IN_UPDATE,				// -348
	INVALID_UPDATE_TYPE,					// -347
	UPDATE_TYPE_MISMATCH,					// -346
	UPDATE_CHAR_LEN_MISMATCH,				// -345
	UPDATE_INT_SIZE_FAILURE,				// -344
	NO_MATCHING_COL_IN_WHERE_OF_UPDATE,		// -343
	INVALID_TYPE_IN_WHERE_OF_UPDATE,		// -342
	MISMATCH_TYPE_IN_WHERE_OF_UPDATE,		// -341
	NO_MATCHING_ROW_TO_UPDATE,				// -340
	UPDATE_NOT_NULL_CONSTRAINT,				// -339

	/* errors for select */
	INVALID_SELECT_STATEMENT = -335,   	// -335
	INVALID_WHERE_CLAUSE_IN_SELECT,		// -334
	INVALID_ORDER_BY_CLAUSE_IN_SELECT,	// -332
	INVALID_SELECT_BY_COLUMN_STATEMENT,	// -331
	INVALID_SYNTAX_FOR_AGGREGATE,		// -330
	INVALID_COL_FOR_AGGREGATE,			// -329
	AGGREGATE_COL_TYPE_MISMATCH,		// -328
	MISMATCH_TYPE_IN_WHERE_OF_SELECT,	// -327
	UNEXPECTED_ITEM_IN_WHERE_OF_SELECT,	// -326
	INVALID_REL_OP_IN_WHERE_OF_SELECT,	// -325
	UNEXPECTED_AFTER_WHERE_OF_SELECT, 	// -324
	
	/* other errors */
	FILE_OPEN_ERROR = -299,		// -299
	DBFILE_CORRUPTION,			// -298
	MEMORY_ERROR,				// -297
	MISSING_IMG_FILENAME,		// -296
	DUPLICATE_IMG_FILENAME,		// -295
	INVALID_BACKUP_STATEMENT,	// -294
	INVALID_RESTORE_STATEMENT,	// -293
	INVALID_ROLLFOWARD_STMT,		// -292
	ROLLFORWARD_PENDING,			// -291
	INVALID_TS_FOR_ROLLFORWARD,		// -290
	MISSING_RF_START_IN_LOG,		// -289
	NOT_READY_FOR_ROLLFORWARD		// -288
} return_codes;

/* Set of function prototypes */
char* get_timestamp();
int get_token(char *command, token_list **tok_list);
void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value);
int do_semantic(token_list *tok_list);
int sem_create_table(token_list *t_list);
int sem_drop_table(token_list *t_list);
int sem_list_tables();
int sem_list_schema(token_list *t_list);
int sem_insert(token_list *t_list);
int sem_select(token_list *t_list);
int sem_delete(token_list *t_list);
int sem_update(token_list *t_list);
int sem_backup(token_list *t_list);
int sem_restore(token_list *t_list);
int sem_rollforward(token_list *t_list);

/* helper functions */
token_list* insertHelper(int t_class, int t_value, char* t_string);
int updateHelper(tpd_entry *tab_entry, int col_to_update, token_list *update_token);
int deleteHelper(char *table_name);
int select_helper_math(tpd_entry *tab_entry, table_file_header *table_info, unsigned char* buffer, int col_to_aggregate, int num_records);
int cnt_not_null(tpd_entry *tab_entry, table_file_header *table_info, unsigned char* buffer, int col_to_cnt, int num_records);
unsigned char* get_buffer(tpd_entry *tab_entry);
table_file_header* getTFH(tpd_entry *tab_entry);

/* printer helper functions */
char* getOuter(tpd_entry *tab_entry);
char* getColHeaders(tpd_entry *tab_entry);
char* getOuterByCol(tpd_entry *tab_entry, int colArray[], int num_col_to_fetch);
char* getColHeadersByCol(tpd_entry *tab_entry, int colArray[], int num_col_to_fetch);
int print_selectAll(tpd_entry *tab_entry);
int print_select(tpd_entry *tab_entry, int colArray[], int cols_to_print);
int print_aggregate(tpd_entry *tab_entry, table_file_header *table_info, unsigned char* buffer, int column_number, token_list *agg_tok, int agg_func, int num_records);
int print_select_from_buffer(tpd_entry *tab_entry, unsigned char* buffer, int len_of_buffer, int num_records, int record_size);
int print_select_from_buffer(tpd_entry *tab_entry, unsigned char* buffer, int len_of_buffer, int matches, int record_size,  int colArray[], int cols_to_print);

unsigned char* orderByBuffer(unsigned char* buffer, tpd_entry *tab_entry, int col_to_order_on, int order, int rec_size, int rec_cnt);

/* checking helper functions */
int columnFinder(tpd_entry *tab_entry, char *tok_string);
int checkColType(tpd_entry *tab_entry, char *tok_string, int t_type, int c_num);
int checkCharLen(tpd_entry *tab_entry, char *tok_string, int c_num);
int checkIntSize(char *tok_string);
int checkRowsForValue(tpd_entry *tab_entry, int col_to_update, token_list *update_token, int rel_op, token_list *where_token, int c_num);
int checkRowsForDelete(tpd_entry *tab_entry, int rel_op, token_list *where_token, int c_num);
int checkAggregateSyntax(tpd_entry *tab_entry, token_list *agg_col);

/* where parser functions */
unsigned char* selectRowsForValue(unsigned char* buffer, tpd_entry *tab_entry, int col_to_search, token_list *search_token, int rel_op, int rec_cnt, int rec_size);
unsigned char* selectRowsForValueOr(unsigned char* buffer, tpd_entry *tab_entry, int col_to_search, token_list *search_token, int rel_op, int rec_cnt, int rec_size, int col_to_search2, token_list *search_token2, int rel_op2);
int getNumberOfMatches(unsigned char* buffer, tpd_entry *tab_entry, int col_to_search, token_list *search_token, int rel_op, int rec_cnt, int rec_size);
int getNumberOfMatchesOr(unsigned char* buffer, tpd_entry *tab_entry, int col_to_search, token_list *search_token, int rel_op, int rec_cnt, int rec_size, int col_to_search2, token_list *search_token2, int rel_op2);

/* other helper functions*/
int restoreHelper(token_list *img_file_name);
int pruneLog(token_list *img_file_name, bool without_rf);

/*
	Keep a global list of tpd - in real life, this will be stored
	in shared memory.  Build a set of functions/methods around this.
*/
tpd_list	*g_tpd_list;
int initialize_tpd_list();
int add_tpd_to_list(tpd_entry *tpd);
int drop_tpd_from_list(char *tabname);
tpd_entry* get_tpd_from_list(char *tabname);