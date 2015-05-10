/************************************************************
	Project#1:	CLP & DDL
 ************************************************************/

#ifdef _MSC_VER
#define _CRT_SECURE_NO_WARNINGS
#endif
#include "db.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <math.h>
#include <time.h>
#include <conio.h>

int main(int argc, char** argv)
{
	int rc = 0;
	token_list *tok_list=NULL, *tok_ptr=NULL, *tmp_tok_ptr=NULL;
	
	if ((argc != 2) || (strlen(argv[1]) == 0))
	{
		printf("Usage: db \"command statement\"");
		return 1;
	}

	rc = initialize_tpd_list();

	if (rc)
	{
		printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
	}
	else
	{
		//regular function
		rc = get_token(argv[1], &tok_list);

		/* Test code */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			//printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class, tok_ptr->tok_value);
			tok_ptr = tok_ptr->next;
		}
    
		if (!rc)
		{	//TODO: ALLOW SELECT AND LIST WHEN IN ROLLFWD PENDING
			if( (g_tpd_list->db_flags == ROLLFORWARD_PENDING) && (tok_list->tok_value != K_ROLLFORWARD) ){
				printf("cannot perform action. rollforward is pending\n");
				rc = ROLLFORWARD_PENDING;
			}
			else if((g_tpd_list->db_flags != ROLLFORWARD_PENDING) && (tok_list->tok_value == K_ROLLFORWARD)){
				printf("cannot perform rollforward. db is not in rollforward state\n");
				rc = NOT_READY_FOR_ROLLFORWARD;
			}
			else{
				rc = do_semantic(tok_list);	
			}
		}

		if (rc)
		{
			tok_ptr = tok_list;
			while (tok_ptr != NULL)
			{
				if ((tok_ptr->tok_class == error) ||
					  (tok_ptr->tok_value == INVALID))
				{
					printf("\nError in the string: %s\n", tok_ptr->tok_string);
					printf("rc=%d\n", rc);
					break;
				}
				tok_ptr = tok_ptr->next;
			}
		}
		else if( (tok_list->tok_value != K_SELECT) && (tok_list->tok_value != K_RESTORE) 
				&& (tok_list->tok_value != K_LIST) && (tok_list->tok_value != K_ROLLFORWARD))
		{	//log all transactions that are not select, list, restore or rollfoward
			FILE *fhandle = NULL;
			char *ts = NULL;
			char *cmd = NULL;
			ts = get_timestamp();

			if((fhandle = fopen("db.log", "a")) != NULL)
			{
				fprintf(fhandle,ts);
				fprintf(fhandle," \"");
				fprintf(fhandle,argv[1]);
				fprintf(fhandle,"\"\n");
				fclose(fhandle);
			}
			else{
				printf("error writing to log file\n");
				rc = FILE_OPEN_ERROR;
			}
		}

		/* Whether the token list is valid or not, we need to free the memory */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
		  tmp_tok_ptr = tok_ptr->next;
		  free(tok_ptr);
		  tok_ptr=tmp_tok_ptr;
		}
	}

	return rc;
}

char* get_timestamp()
{
	time_t curtime;
	struct tm *the_tm; //gets the time
	curtime = time(NULL);
	the_tm = localtime(&curtime); //get localtime
	char *timestamp = NULL; //
	char temp[4];
	timestamp = (char*)calloc(1, 16);
	//printf("%d %02d %02d %02d %02d %02d", the_tm->tm_year+1900, the_tm->tm_mon+1, the_tm->tm_mday, the_tm->tm_hour, the_tm->tm_min, the_tm->tm_sec);

	itoa(the_tm->tm_year+1900, timestamp, 10);//convert year into str
	itoa(the_tm->tm_mon+1, temp, 10); //convert month into str
	if(strlen(temp)<2){//add leading zero if only 1 digit (1-9)
		strcat(timestamp,"0"); 
	}
	strcat(timestamp, temp); //add month to ts string

	itoa(the_tm->tm_mday, temp, 10);
	if(strlen(temp)<2){
		strcat(timestamp,"0");
	}
	strcat(timestamp, temp);

	itoa(the_tm->tm_hour, temp, 10);
	if(strlen(temp)<2){
		strcat(timestamp,"0");
	}
	strcat(timestamp, temp);

	itoa(the_tm->tm_min, temp, 10);
	if(strlen(temp)<2){
		strcat(timestamp,"0");
	}
	strcat(timestamp, temp);

	itoa(the_tm->tm_sec, temp, 10);
	if(strlen(temp)<2){
		strcat(timestamp,"0");
	}
	strcat(timestamp, temp);
	//printf("ts: %s\n", timestamp);
	return timestamp;
}

/************************************************************* 
	This is a lexical analyzer for simple SQL statements
 *************************************************************/
int get_token(char* command, token_list** tok_list)
{
	int rc=0,i,j;
	char *start, *cur, temp_string[MAX_TOK_LEN];
	bool done = false;
	
	start = cur = command;
	while (!done)
	{
		bool found_keyword = false;

		/* This is the TOP Level for each token */
		memset ((void*)temp_string, '\0', MAX_TOK_LEN);
		i = 0;

		/* Get rid of all the leading blanks */
		while (*cur == ' ')
			cur++;

		if (cur && isalpha(*cur))
		{	// find valid identifier
			int t_class;
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((isalnum(*cur)) || (*cur == '_'));

			if (!(strchr(STRING_BREAK, *cur)))
			{	/* If the next char following the keyword or identifier
				   is not a blank, (, ), or a comma, then append this
				   character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{	// We have an identifier with at least 1 character
				// Now check if this ident is a keyword
				for (j = 0, found_keyword = false; j < TOTAL_KEYWORDS_PLUS_TYPE_NAMES; j++)
				{
					if ((_stricmp(keyword_table[j], temp_string) == 0))
					{
						found_keyword = true;
						break;
					}
				}

				if (found_keyword)
				{
				  if (KEYWORD_OFFSET+j < K_CREATE)
						t_class = type_name;
					else if (KEYWORD_OFFSET+j >= F_SUM)
						t_class = function_name;
					else
					  t_class = keyword;

					add_to_list(tok_list, temp_string, t_class, KEYWORD_OFFSET+j);
				}
				else
				{
					if (strlen(temp_string) <= MAX_IDENT_LEN)
					  add_to_list(tok_list, temp_string, identifier, IDENT);
					else
					{
						add_to_list(tok_list, temp_string, error, INVALID);
						rc = INVALID;
						done = true;
					}
				}

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}//end isalpha
		else if (isdigit(*cur))
		{	// find valid number
			do 
			{
				temp_string[i++] = *cur++;
			}
			while (isdigit(*cur));

			if (!(strchr(NUMBER_BREAK, *cur)))
			{	/* If the next char following the keyword or identifier
				   is not a blank or a ), then append this
				   character to temp_string, and flag this as an error */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else
			{
				add_to_list(tok_list, temp_string, constant, INT_LITERAL);

				if (!*cur)
				{
					add_to_list(tok_list, "", terminator, EOC);
					done = true;
				}
			}
		}//end isdigit
		else if ((*cur == '(') || (*cur == ')') || (*cur == ',') || (*cur == '*')
		         || (*cur == '=') || (*cur == '<') || (*cur == '>'))
		{	/* Catch all the symbols here. Note: no look ahead here. */
			int t_value;
			switch (*cur)
			{
				case '(' : t_value = S_LEFT_PAREN; break;
				case ')' : t_value = S_RIGHT_PAREN; break;
				case ',' : t_value = S_COMMA; break;
				case '*' : t_value = S_STAR; break;
				case '=' : t_value = S_EQUAL; break;
				case '<' : t_value = S_LESS; break;
				case '>' : t_value = S_GREATER; break;
			}

			temp_string[i++] = *cur++;
			add_to_list(tok_list, temp_string, symbol, t_value);

			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
		}//end symbol catch
		else if (*cur == '\'')
		{	/* Find STRING_LITERAL */
			int t_class;
			cur++;
			
			do 
			{
				temp_string[i++] = *cur++;
			}
			while ((*cur) && (*cur != '\''));

			temp_string[i] = '\0';

			if (!*cur)
			{	/* If we reach the end of line */
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
			else /* must be a ' */
			{
				add_to_list(tok_list, temp_string, constant, STRING_LITERAL);
				cur++;
					if (!*cur)
					{
						add_to_list(tok_list, "", terminator, EOC);
						done = true;
					}
			}
		}//end string literal
		else
		{
			if (!*cur)
			{
				add_to_list(tok_list, "", terminator, EOC);
				done = true;
			}
			else
			{	/* not a ident, number, or valid symbol */
				temp_string[i++] = *cur++;
				add_to_list(tok_list, temp_string, error, INVALID);
				rc = INVALID;
				done = true;
			}
		}//end else for parser
	}//end else while for !done
	
	return rc;
}

void add_to_list(token_list **tok_list, char *tmp, int t_class, int t_value)
{
	token_list *cur = *tok_list;
	token_list *ptr = NULL;

	// printf("%16s \t%d \t %d\n",tmp, t_class, t_value);

	ptr = (token_list*)calloc(1, sizeof(token_list));
	//printf("size of tlist: %d\n", sizeof(token_list));
	strcpy(ptr->tok_string, tmp);

	ptr->tok_class = t_class;
	ptr->tok_value = t_value;
	ptr->next = NULL;

	if (cur == NULL)
		*tok_list = ptr;
	else
	{
		while (cur->next != NULL)
			cur = cur->next;

		cur->next = ptr;
	}
	return;
}

int do_semantic(token_list *tok_list)
{
	int rc = 0, cur_cmd = INVALID_STATEMENT;
	bool unique = false;
	token_list *cur = tok_list;

	if ((cur->tok_value == K_CREATE) &&
			((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("CREATE TABLE statement\n");
		cur_cmd = CREATE_TABLE;
		cur = cur->next->next;	
	}//end k_create
	else if ((cur->tok_value == K_DROP) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("DROP TABLE statement\n");
		cur_cmd = DROP_TABLE;
		cur = cur->next->next;
	}// end k_drop
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_TABLE)))
	{
		printf("LIST TABLE statement\n");
		cur_cmd = LIST_TABLE;
		cur = cur->next->next;
	}// end k_list for table
	else if ((cur->tok_value == K_LIST) &&
					((cur->next != NULL) && (cur->next->tok_value == K_SCHEMA)))
	{
		printf("LIST SCHEMA statement\n");
		cur_cmd = LIST_SCHEMA;
		cur = cur->next->next;
	}// end k_list for schema
	else if ((cur->tok_value == K_INSERT) &&
					((cur->next != NULL) && (cur->next->tok_value == K_INTO)))
	{
		printf("INSERT INTO statement\n");
		cur_cmd = INSERT;
		cur = cur->next->next;
	}//end k_insert
	else if ((cur->tok_value == K_SELECT) && (cur->next != NULL))
	{
		printf("SELECT statement\n");
		cur_cmd = SELECT;
		cur = cur->next;
	} //end k_select
	else if ((cur->tok_value == K_DELETE) && 
					(cur->next != NULL) && (cur->next->tok_value == K_FROM))
	{
		printf("DELETE FROM statement\n");
		cur_cmd = DELETE;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_UPDATE) && (cur->next != NULL))
	{
		printf("UPDATE statement\n");
		cur_cmd = UPDATE;
		cur = cur->next;
	}
	else if ((cur->tok_value == K_BACKUP) && (cur->next->tok_value == K_TO))
	{
		printf("BACKUP statement\n");
		cur_cmd = BACKUP;
		cur = cur->next->next;
	}
	else if ((cur->tok_value == K_RESTORE) && (cur->next->tok_value == K_FROM))
	{
		printf("RESTORE statement\n");
		cur_cmd = RESTORE;
		cur = cur->next->next;
	}
	else if (cur->tok_value == K_ROLLFORWARD)
	{
		printf("ROLLFORWARD statement\n");
		cur_cmd = ROLLFORWARD;
		cur = cur->next;
	}
	else
	{
		printf("Invalid statement\n");
		rc = cur_cmd;
	}//end else (invalid)

	if (cur_cmd != INVALID_STATEMENT)
	{
		switch(cur_cmd)
		{
			case CREATE_TABLE:
				rc = sem_create_table(cur);
				break;
			case DROP_TABLE:
				rc = sem_drop_table(cur);
				break;
			case LIST_TABLE:
				rc = sem_list_tables();
				break;
			case LIST_SCHEMA:
				rc = sem_list_schema(cur);
				break;
			case INSERT:
				rc = sem_insert(cur); 
				break;
			case SELECT:
				rc = sem_select(cur);
				break;
			case DELETE:
				rc = sem_delete(cur);
				break;
			case UPDATE:
				rc = sem_update(cur);
				break;
			case BACKUP:
				rc = sem_backup(cur);
				break;
			case RESTORE:
				rc = sem_restore(cur);
				break;
			case ROLLFORWARD:
				rc = sem_rollforward(cur);
				break;
			default:
				; /* no action */
		}//end switch
	}//end !invalid stmt
	
	return rc;
}

int initialize_tpd_list()
{
	int rc = 0;
	FILE *fhandle = NULL;
	struct _stat file_stat;

	/* Open for read */
	if((fhandle = fopen("dbfile.bin", "rbc")) == NULL)
	{
		if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
		{
			rc = FILE_OPEN_ERROR;
		}
		else
		{
			g_tpd_list = NULL;
			g_tpd_list = (tpd_list*)calloc(1, sizeof(tpd_list));
			
			if (!g_tpd_list)
			{
				rc = MEMORY_ERROR;
			}
			else
			{
				g_tpd_list->list_size = sizeof(tpd_list);
				fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
			}
		}
	}
	else
	{
		/* There is a valid dbfile.bin file - get file size */
		_fstat(_fileno(fhandle), &file_stat);
		printf("dbfile.bin size = %d\n", file_stat.st_size);

		g_tpd_list = (tpd_list*)calloc(1, file_stat.st_size);

		if (!g_tpd_list)
		{
			rc = MEMORY_ERROR;
		}
		else
		{
			fread(g_tpd_list, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);

			if (g_tpd_list->list_size != file_stat.st_size)
			{
				rc = DBFILE_CORRUPTION;
			}

		}
	}
    
	return rc;
}
	
int add_tpd_to_list(tpd_entry *tpd)
{
	int rc = 0;
	int old_size = 0;
	FILE *fhandle = NULL;

	if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
	{
		rc = FILE_OPEN_ERROR;
	}
	else
	{
		old_size = g_tpd_list->list_size;

		if (g_tpd_list->num_tables == 0)
		{
			/* If this is an empty list, overlap the dummy header */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += (tpd->tpd_size - sizeof(tpd_entry));
			fwrite(g_tpd_list, old_size - sizeof(tpd_entry), 1, fhandle);
		}
		else
		{
			/* There is at least 1, just append at the end */
			g_tpd_list->num_tables++;
		 	g_tpd_list->list_size += tpd->tpd_size;
			fwrite(g_tpd_list, old_size, 1, fhandle);
		}

		fwrite(tpd, tpd->tpd_size, 1, fhandle);
		fflush(fhandle);
		fclose(fhandle);
	}

	return rc;
}

int drop_tpd_from_list(char *tabname)
{
	int rc = 0;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;
	int count = 0;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (_stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				int old_size = 0;
				FILE *fhandle = NULL;

				if((fhandle = fopen("dbfile.bin", "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
				else
				{
					old_size = g_tpd_list->list_size;

					if (count == 0)
					{
						/* If this is the first entry */
						g_tpd_list->num_tables--;

						if (g_tpd_list->num_tables == 0)
						{
							/* This is the last table, null out dummy header */
							memset((void*)g_tpd_list, '\0', sizeof(tpd_list));
							g_tpd_list->list_size = sizeof(tpd_list);
							fwrite(g_tpd_list, sizeof(tpd_list), 1, fhandle);
						}
						else
						{
							/* First in list, but not the last one */
							g_tpd_list->list_size -= cur->tpd_size;

							/* First, write the 8 byte header */
							fwrite(g_tpd_list, sizeof(tpd_list) - sizeof(tpd_entry),
								     1, fhandle);

							/* Now write everything starting after the cur entry */
							fwrite((char*)cur + cur->tpd_size,
								     old_size - cur->tpd_size -
										 (sizeof(tpd_list) - sizeof(tpd_entry)),
								     1, fhandle);
						}
					}
					else
					{
						/* This is NOT the first entry - count > 0 */
						g_tpd_list->num_tables--;
					 	g_tpd_list->list_size -= cur->tpd_size;

						/* First, write everything from beginning to cur */
						fwrite(g_tpd_list, ((char*)cur - (char*)g_tpd_list),
									 1, fhandle);

						/* Check if cur is the last entry. Note that g_tdp_list->list_size
						   has already subtracted the cur->tpd_size, therefore it will
						   point to the start of cur if cur was the last entry */
						if ((char*)g_tpd_list + g_tpd_list->list_size == (char*)cur)
						{
							/* If true, nothing else to write */
						}
						else
						{
							/* NOT the last entry, copy everything from the beginning of the
							   next entry which is (cur + cur->tpd_size) and the remaining size */
							fwrite((char*)cur + cur->tpd_size,
										 old_size - cur->tpd_size -
										 ((char*)cur - (char*)g_tpd_list),							     
								     1, fhandle);
						}
					}

					fflush(fhandle);
					fclose(fhandle);
				}
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
					count++;
				}
			}
		}
	}
	
	if (!found)
	{
		rc = INVALID_TABLE_NAME;
	}

	return rc;
}

tpd_entry* get_tpd_from_list(char *tabname)
{
	tpd_entry *tpd = NULL;
	tpd_entry *cur = &(g_tpd_list->tpd_start);
	int num_tables = g_tpd_list->num_tables;
	bool found = false;

	if (num_tables > 0)
	{
		while ((!found) && (num_tables-- > 0))
		{
			if (_stricmp(cur->table_name, tabname) == 0)
			{
				/* found it */
				found = true;
				tpd = cur;
			}
			else
			{
				if (num_tables > 0)
				{
					cur = (tpd_entry*)((char*)cur + cur->tpd_size);
				}
			}
		}
	}

	return tpd;
}

int sem_create_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *new_entry = NULL;
	bool column_done = false;
	int cur_id = 0;
	cd_entry	col_entry[MAX_NUM_COL];
	int rec_size = 0;

	memset(&tab_entry, '\0', sizeof(tpd_entry));
	cur = t_list;
	if ((cur->tok_class != keyword) && (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{	// Error
		printf("invalid table name\n");
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if ((new_entry = get_tpd_from_list(cur->tok_string)) != NULL)
		{
			printf("duplicate table name\n");
			rc = DUPLICATE_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			strcpy(tab_entry.table_name, cur->tok_string);
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{	//Error
				printf("missing ( for create table statement\n");
				rc = INVALID_TABLE_DEFINITION;
				cur->tok_value = INVALID;
			}
			else
			{
				memset(&col_entry, '\0', (MAX_NUM_COL * sizeof(cd_entry)));

				/* Now build a set of column entries */
				cur = cur->next;
				do
				{
					if ((cur->tok_class != keyword) && (cur->tok_class != identifier) &&
						(cur->tok_class != type_name))
					{	// Error
						printf("invalid column name\n");
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						int i;
						for(i = 0; i < cur_id; i++)
						{	/* make column name case sensitive */
							if (strcmp(col_entry[i].col_name, cur->tok_string)==0)
							{
								printf("duplicate column name\n");
								rc = DUPLICATE_COLUMN_NAME;
								cur->tok_value = INVALID;
								break;
							}
						}

						if (!rc)
						{
							strcpy(col_entry[cur_id].col_name, cur->tok_string);
							col_entry[cur_id].col_id = cur_id;
							col_entry[cur_id].not_null = false;    /* set default */

							cur = cur->next;
							if (cur->tok_class != type_name)
							{	// Error
								printf("invalid column type (only int and char(#) are supported)\n");
								rc = INVALID_TYPE_NAME;
								cur->tok_value = INVALID;
							}
							else
							{	/* Set the column type here, int or char */
								col_entry[cur_id].col_type = cur->tok_value;
								
								cur = cur->next;
		
								if (col_entry[cur_id].col_type == T_INT)
								{
									if ((cur->tok_value != S_COMMA) && (cur->tok_value != K_NOT) &&
										  (cur->tok_value != S_RIGHT_PAREN))
									{
										printf("invalid column definition. check for missing , or ).\n");
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{	//for int
										col_entry[cur_id].col_len = sizeof(int);
										rec_size += col_entry[cur_id].col_len + 1;
										
										if ((cur->tok_value == K_NOT) && (cur->next->tok_value != K_NULL))
										{
											printf("invalid use of 'not' keyword\n");
											rc = INVALID_COLUMN_DEFINITION;
											cur->tok_value = INVALID;
										}	
										else if ((cur->tok_value == K_NOT) &&
											    (cur->next->tok_value == K_NULL))
										{					
											col_entry[cur_id].not_null = true;
											cur = cur->next->next;
										}
	
										if (!rc)
										{
											/* I must have either a comma or right paren */
											if ((cur->tok_value != S_RIGHT_PAREN) && (cur->tok_value != S_COMMA))
											{
												printf("missing ) or , in create table statement\n");
												rc = INVALID_COLUMN_DEFINITION;
												cur->tok_value = INVALID;
											}
											else
											{
												if (cur->tok_value == S_RIGHT_PAREN)
												{
 													column_done = true;
												}
												cur = cur->next;
											}
										}
									}
								}// end of S_INT processing
								else
								{	// It must be char()
									if (cur->tok_value != S_LEFT_PAREN)
									{
										printf("invalid column definition\n");
										rc = INVALID_COLUMN_DEFINITION;
										cur->tok_value = INVALID;
									}
									else
									{	/* Enter char(n) processing */
										cur = cur->next;
		
										if (cur->tok_value != INT_LITERAL)
										{
											printf("invalid length for char\n");
											rc = INVALID_COLUMN_LENGTH;
											cur->tok_value = INVALID;
										}
										else
										{	/* Got a valid integer - convert */
											col_entry[cur_id].col_len = atoi(cur->tok_string);
											if(col_entry[cur_id].col_len > MAX_TOK_LEN)
											{
												printf("char length cannot exceed 32\n");
												rc = INVALID_COLUMN_LENGTH;
												cur->tok_value = INVALID;
											}
											else 
											{	//char length <= 32
												rec_size += col_entry[cur_id].col_len + 1;
												cur = cur->next;
												
												if (cur->tok_value != S_RIGHT_PAREN)
												{
													printf("missing ) for char definition\n");
													rc = INVALID_COLUMN_DEFINITION;
													cur->tok_value = INVALID;
												}
												else
												{
													cur = cur->next;
													if ((cur->tok_value != S_COMMA) && (cur->tok_value != K_NOT) &&
															(cur->tok_value != S_RIGHT_PAREN))
													{
														rc = INVALID_COLUMN_DEFINITION;
														cur->tok_value = INVALID;
													}
													else
													{
														if ((cur->tok_value == K_NOT) && (cur->next->tok_value != K_NULL))
														{
															printf("invalid use of 'not' keyword\n");
															rc = INVALID_COLUMN_DEFINITION;
															cur->tok_value = INVALID;
														}
														else if ((cur->tok_value == K_NOT) && (cur->next->tok_value == K_NULL))
														{					
															col_entry[cur_id].not_null = true;
															cur = cur->next->next;
														}
			
														if (!rc)
														{
															/* I must have either a comma or right paren */
															if ((cur->tok_value != S_RIGHT_PAREN) && (cur->tok_value != S_COMMA))
															{
																printf("invalid column definition. check for missing , or ).\n");
																rc = INVALID_COLUMN_DEFINITION;
																cur->tok_value = INVALID;
															}
															else
															{
																if (cur->tok_value == S_RIGHT_PAREN)
																{
																	column_done = true;
																}
																cur = cur->next;
															}
														}
													}
												}
											}
											
										}	/* end char(n) processing */
									}
								} /* end char processing */
							}
						}  // duplicate column name
					} // invalid column name

					/* If rc=0, then get ready for the next column */
					if (!rc)
					{
						cur_id++;
					}

				} while ((rc == 0) && (!column_done));
	
				if ((column_done) && (cur->tok_value != EOC))
				{
					rc = INVALID_TABLE_DEFINITION;
					cur->tok_value = INVALID;
				}

				if (!rc)
				{
					/* Now finished building tpd and add it to the tpd list */
					tab_entry.num_columns = cur_id;
					tab_entry.tpd_size = sizeof(tpd_entry) + sizeof(cd_entry) *	tab_entry.num_columns;
					tab_entry.cd_offset = sizeof(tpd_entry);
					new_entry = (tpd_entry*)calloc(1, tab_entry.tpd_size);

					if (new_entry == NULL)
					{
						printf("there is a memory error...\n");
						rc = MEMORY_ERROR;
					}
					else
					{	
						memcpy((void*)new_entry,
							     (void*)&tab_entry,
									 sizeof(tpd_entry));
		
						memcpy((void*)((char*)new_entry + sizeof(tpd_entry)),
									 (void*)col_entry,
									 sizeof(cd_entry) * tab_entry.num_columns);
						rc = add_tpd_to_list(new_entry);

						free(new_entry);

						// Round rec_size to higher 4-byte boundary
						int rem = rec_size % 4;
						if (rem != 0)
							rec_size = (int)ceil(rec_size / 4.0) * 4;

						//Create <table_name>.tab file
						char str[MAX_IDENT_LEN + 1];
						strcpy(str, tab_entry.table_name); //get table name
						strcat(str, ".tab");

						FILE *fhandle = NULL;
						
						//Create and open file for read
						if ((fhandle = fopen(str, "wbc")) == NULL)
						{
							printf("there is an error opening the file %s\n", str);
							rc = FILE_OPEN_ERROR;
						}
						else
						{
							table_file_header ptr;
							memset(&ptr, '\0', sizeof(table_file_header));
							ptr.record_size = rec_size;
							ptr.num_records = 0; //no records yet
							ptr.file_size = sizeof(table_file_header);
							ptr.record_offset = sizeof(table_file_header);
							ptr.file_header_flag = 0; //TODO: what is this for?
							ptr.tpd_ptr = NULL;

							table_file_header *cur = NULL;
							cur = (table_file_header*)calloc(1, sizeof(table_file_header));

							if (cur == NULL)
							{
								printf("there is a memory error...\n");
								rc = MEMORY_ERROR;
							}
							else
							{
								memcpy((void*)cur, (void*)&ptr, sizeof(table_file_header));
								fwrite(cur, sizeof(table_file_header), 1, fhandle);
								fflush(fhandle);
								fclose(fhandle);

								free(cur);
							}
							printf("tablename is %s\n", str);
						} //file opened for write
					} //end new_entry != NULL
				}//end if !rc
			}
		} //table is unique (not duplicate)
	} //table name is valid

	return rc;
}

int sem_drop_table(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;

	cur = t_list;
	if ((cur->tok_class != keyword) &&
		  (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{
		// Error
		rc = INVALID_TABLE_NAME;
		cur->tok_value = INVALID;
	}
	else
	{
		if (cur->next->tok_value != EOC)
		{
			rc = INVALID_STATEMENT;
			cur->next->tok_value = INVALID;
		}
		else
		{
			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{
				/* Found a valid tpd, drop it from tpd list */
				rc = drop_tpd_from_list(cur->tok_string);
				
				/* Delete <table_name>.tab file*/
				char str[MAX_IDENT_LEN + 4 + 3];
				strcpy(str, cur->tok_string); //get table name
				strcat(str, ".tab"); 
				printf("Deleting...%s\n", str);
				if (remove(str) != 0)
					perror("Error deleting file\n");
				else
					printf("Done deleting %s\n", str);
			}
		}
	}

  return rc;
}

int sem_list_tables()
{
	int rc = 0;
	int num_tables = g_tpd_list->num_tables;
	tpd_entry *cur = &(g_tpd_list->tpd_start);

	if (num_tables == 0)
	{
		printf("\nThere are currently no tables defined\n");
	}
	else
	{
		printf("\nTable List\n");
		printf("*****************\n");
		while (num_tables-- > 0)
		{
			printf("%s\n", cur->table_name);
			if (num_tables > 0)
			{
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}
		}
		printf("****** End ******\n");
	}

  return rc;
}

int sem_list_schema(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	cd_entry  *col_entry = NULL;
	char tab_name[MAX_IDENT_LEN+1];
	char filename[MAX_IDENT_LEN+1];
	bool report = false;
	FILE *fhandle = NULL;
	int i = 0;

	cur = t_list;

	if (cur->tok_value != K_FOR)
	{
		rc = INVALID_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		cur = cur->next;

		if ((cur->tok_class != keyword) &&
			  (cur->tok_class != identifier) &&
				(cur->tok_class != type_name))
		{
			// Error
			rc = INVALID_TABLE_NAME;
			cur->tok_value = INVALID;
		}
		else
		{
			memset(filename, '\0', MAX_IDENT_LEN+1);
			strcpy(tab_name, cur->tok_string);
			cur = cur->next;

			if (cur->tok_value != EOC)
			{
				if (cur->tok_value == K_TO)
				{
					cur = cur->next;
					
					if ((cur->tok_class != keyword) &&
						  (cur->tok_class != identifier) &&
							(cur->tok_class != type_name))
					{
						// Error
						rc = INVALID_REPORT_FILE_NAME;
						cur->tok_value = INVALID;
					}
					else
					{
						if (cur->next->tok_value != EOC)
						{
							rc = INVALID_STATEMENT;
							cur->next->tok_value = INVALID;
						}
						else
						{
							/* We have a valid file name */
							strcpy(filename, cur->tok_string);
							report = true;
						}
					}
				}
				else
				{ 
					/* Missing the TO keyword */
					rc = INVALID_STATEMENT;
					cur->tok_value = INVALID;
				}
			}

			if (!rc)
			{
				if ((tab_entry = get_tpd_from_list(tab_name)) == NULL)
				{
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					if (report)
					{
						if((fhandle = fopen(filename, "a+tc")) == NULL)
						{
							rc = FILE_OPEN_ERROR;
						}
					}

					if (!rc)
					{
						/* Find correct tpd, need to parse column and index information */

						/* First, write the tpd_entry information */
						printf("Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
						printf("Table Name               (table_name)  = %s\n", tab_entry->table_name);
						printf("Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
						printf("Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
						printf("Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 

						if (report)
						{
							fprintf(fhandle, "Table PD size            (tpd_size)    = %d\n", tab_entry->tpd_size);
							fprintf(fhandle, "Table Name               (table_name)  = %s\n", tab_entry->table_name);
							fprintf(fhandle, "Number of Columns        (num_columns) = %d\n", tab_entry->num_columns);
							fprintf(fhandle, "Column Descriptor Offset (cd_offset)   = %d\n", tab_entry->cd_offset);
							fprintf(fhandle, "Table PD Flags           (tpd_flags)   = %d\n\n", tab_entry->tpd_flags); 
						}

						/* Next, write the cd_entry information */
						for(i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								i < tab_entry->num_columns; i++, col_entry++)
						{
							printf("Column Name   (col_name) = %s\n", col_entry->col_name);
							printf("Column Id     (col_id)   = %d\n", col_entry->col_id);
							printf("Column Type   (col_type) = %d\n", col_entry->col_type);
							printf("Column Length (col_len)  = %d\n", col_entry->col_len);
							printf("Not Null flag (not_null) = %d\n\n", col_entry->not_null);

							if (report)
							{
								fprintf(fhandle, "Column Name   (col_name) = %s\n", col_entry->col_name);
								fprintf(fhandle, "Column Id     (col_id)   = %d\n", col_entry->col_id);
								fprintf(fhandle, "Column Type   (col_type) = %d\n", col_entry->col_type);
								fprintf(fhandle, "Column Length (col_len)  = %d\n", col_entry->col_len);
								fprintf(fhandle, "Not Null Flag (not_null) = %d\n\n", col_entry->not_null);
							}
						}
	
						if (report)
						{
							fflush(fhandle);
							fclose(fhandle);
						}
					} // File open error							
				} // Table not exist
			} // no semantic errors
		} // Invalid table name
	} // Invalid statement

  return rc;
}

int sem_insert(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry tab_entry;
	tpd_entry *exist_entry = NULL;
	table_file_header *ptr = NULL;
	struct _stat file_stat;
	token_list *values = NULL, *head = NULL; //for insert token list
	
	cur = t_list; //get current token list
	//check that identifier table name exists
	if ((exist_entry = get_tpd_from_list(cur->tok_string)) == NULL)
	{
		printf("cannot insert into nonexistent table\n");
		rc = TABLE_NOT_EXIST;
		cur->tok_value = INVALID;
	} //table does not exist
	else 
	{	//table exists, check for values keyword
		cur = cur->next;
		if (cur->tok_value != K_VALUES)
		{	//Error
			printf("values keyword missing\n");
			rc = INVALID_INSERT_STATEMENT;
			cur->tok_value = INVALID;
		}
		else 
		{	//values keyword there, check for R parantheses
			cur = cur->next;
			if (cur->tok_value != S_LEFT_PAREN)
			{	//Error
				printf("missing ( for insert statement\n");
				rc = INVALID_INSERT_STATEMENT;
				cur->tok_value = INVALID;
			}
			else
			{	//L parantheses there, start col and val comparison
				cur = cur->next;
				cd_entry  *col_entry = NULL;
				int i;
				for (i = 0, col_entry = (cd_entry*)((char*)exist_entry + exist_entry->cd_offset);
					i < exist_entry->num_columns; i++, col_entry++)
				{	//while there are columns in tpd
					//else, if token list is not shorter than number of columns, continue parsing
					if (cur->tok_value == S_RIGHT_PAREN)
					{
						printf("number of columns and insert values don't match\n");
						rc = INSERT_VAL_COL_LEN_MISMATCH;
						cur->tok_value = INVALID;
						return rc;
					}
					else if ( (cur->tok_value == INT_LITERAL) || (cur->tok_value == STRING_LITERAL) || (cur->tok_value == K_NULL))
					{
						if ((cur->tok_value == INT_LITERAL) && (col_entry->col_type == T_INT))
						{
							//check that int literal is not greater than max int
							if(!checkIntSize(cur->tok_string))
							{
								printf("integer value exceeds max integer value\n");
								rc = INSERT_INT_SIZE_FAILURE;
								cur->tok_value = INVALID;
								return rc;
							}
							else
							{
								//temporary token holder
								token_list *temp = insertHelper(cur->tok_class, cur->tok_value, cur->tok_string);

								//set values linked list
								if (values == NULL)
								{
									values = temp;
									head = temp;
								}
								else
								{
									head->next = temp;
									head = temp;
								}
							}
						}//end int literal
						else if ((cur->tok_value == STRING_LITERAL) && (col_entry->col_type == T_CHAR))
						{	//for char or string literal
							int str_size = strlen(cur->tok_string);

							if (str_size <= col_entry->col_len)
							{
								//temporary token holder
								//**1st parm = col_len
								token_list *temp = insertHelper(col_entry->col_len, cur->tok_value, cur->tok_string);

								//set values linked list
								if (values == NULL)
								{
									values = temp;
									head = temp;
								}
								else
								{
									head->next = temp;
									head = temp;
								}
							}
							else
							{	//str size too long
								printf("insert char length does not match column defintion.\n");
								rc = INSERT_CHAR_LENGTH_MISMATCH;
								cur->tok_value = INVALID;
								return rc;
							}
						}//end string literal
						else if (cur->tok_value == K_NULL)
						{	//Check for not null constraint
							if (col_entry->not_null)
							{
								printf("not null constraint\n");
								rc = INSERT_NULL_FAILURE;
								cur->tok_value = INVALID;
								return rc;
							}
							else //nullable
							{	
								//temporary token holder
								//**1st parm = col_len
								token_list *temp = insertHelper(col_entry->col_len, cur->tok_value, cur->tok_string);

								//set values linked list
								if (values == NULL)
								{
									values = temp;
									head = temp;
								}
								else
								{
									head->next = temp;
									head = temp;
								}
							}
						}//end null
						else
						{
							printf("type mismatch\n");
							rc = INSERT_VALUE_TYPE_MISMATCH;
							cur->tok_value = INVALID;
							return rc;
						}
						cur = cur->next;
					}
					else
					{
						printf("invalid column value. only char(n) and int are supported.\n");
						rc = INSERT_INVALID_TYPE;
						cur->tok_value = INVALID;
						return rc;
					}

					//parse for comma
					if (cur->tok_value == S_COMMA)
						cur = cur->next; //continue
				}//end for

				if (cur->tok_value == S_RIGHT_PAREN) //got R parentheses
					cur = cur->next;
				else 
				{
					printf("missing ) for insert statement\n");
					rc = INVALID_INSERT_STATEMENT;
					cur->tok_value = INVALID;
					return rc;
				}
			}//end else col/val comparison
		}//end else at L parentheses
	}//end else where table name found

	//do insert (write to .tab)
	if (!rc)
	{	//col/values match tpd columns definitions
		if(cur->tok_value == EOC)
		{	// Write to <table_name>.tab file
			char str[MAX_IDENT_LEN + 1];
			strcpy(str, exist_entry->table_name); //get table name
			strcat(str, ".tab");
			FILE *fhandle = NULL;
			
			//Open file for read
			if ((fhandle = fopen(str, "rbc")) == NULL)
			{
				rc = FILE_OPEN_ERROR;
			}//end file open error
			else
			{
				_fstat(_fileno(fhandle), &file_stat);
				ptr = (table_file_header*)calloc(1, file_stat.st_size);
				
				if (!ptr)
				{
					rc = MEMORY_ERROR;
				}//end memory error
				else
				{
					fread(ptr, file_stat.st_size, 1, fhandle);
					fflush(fhandle);
					fclose(fhandle);

					if (ptr->file_size != file_stat.st_size)
					{
						printf("%s file_size: %d\n", str, ptr->file_size);
						printf("in db file corruption\n");
						rc = DBFILE_CORRUPTION;
					}
					else
					{
						int old_size = 0;
						old_size = ptr->file_size;
						int rec_offset = ptr->record_offset;
						int numb_recs = ptr->num_records;
						
						if(numb_recs >= 100)
						{
							printf("maximum num records of 100 reached. cannot insert more tuples\n");
							rc = MAX_NUM_RECORDS;
							cur->tok_value = INVALID;
						}
						else
						{
							//open file for write to update file header and add new row entries
							if ((fhandle = fopen(str, "wbc")) == NULL)
							{
								rc = FILE_OPEN_ERROR;
								return rc;
							}
							else
							{	//update table file header contents
								ptr->num_records++;
								ptr->file_size += ptr->record_size; //add one record size
								ptr->tpd_ptr = NULL;
								printf("%s new size = %d\n", str, ptr->file_size);

								//write updated table file header
								fwrite(ptr, file_stat.st_size, 1, fhandle);

								//write new row entry
								token_list *llist = values;
								while (llist != NULL)
								{
									unsigned char item_len = 0;
									if (llist->tok_value == STRING_LITERAL)
									{
										item_len = strlen(llist->tok_string);
										//hack to get len of item (tok_class)
										fwrite(&item_len, sizeof(unsigned char), 1, fhandle);
										fwrite(&llist->tok_string, llist->tok_class, 1, fhandle);
									}
									else if (llist->tok_value == INT_LITERAL)
									{
										int item = atoi(llist->tok_string);
										item_len = sizeof(int); //4 bytes for int
										fwrite(&item_len, sizeof(unsigned char), 1, fhandle);
										fwrite(&item, item_len, 1, fhandle);
									}
									else if (llist->tok_value == K_NULL)
									{
										item_len = 0;
										int counter = llist->tok_class; //hack to get null'ed col_len
										int item = 0; //zero out column
										fwrite(&item_len, sizeof(unsigned char), 1, fhandle);
										while (counter > 0)
										{
											fwrite(&item, sizeof(unsigned char), 1, fhandle);
											counter--;
										}
									}
									llist = llist->next;
								}//end while
								
								//get current file size
								fseek(fhandle, 0, SEEK_END);
								int cur_size = ftell(fhandle);

								//padding rec with zeroes if rec_size was rounded up to 4 byte boundary
								while (cur_size < ptr->file_size)
								{
									int pad = 0;
									fwrite(&pad, sizeof(unsigned char), 1, fhandle);
									cur_size++;
								}

								fflush(fhandle);
								fclose(fhandle);
							}//end file open for write
						} // < 100 num_records, can insert
					}//end check that file size is correct
				}//end not memory error
			}//end file open for read
		}//at EOC
		else
		{	// there is extra stuff after R parantheses or extra values given
			printf("invalid insert statement\n");
			rc = INVALID_INSERT_STATEMENT;
			cur->tok_value = INVALID;
		}
	}//end insert
	return rc;
}

int sem_select(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	unsigned char *the_buffer = NULL, *filtered_buffer = NULL, *second_filter_buffer = NULL, *ordered_buffer = NULL;
	table_file_header* table_info = NULL;
	int colArray[MAX_NUM_COL]; //array to hold column ids to select
	int index = 0; //array index counter for colArray
	int aggregate_func = 0; //holds type of agg func
	int aggregate_col = -1; //holds the col that agg func should operate on
	token_list *aggregate_col_tok = NULL, *where_col1_tok = NULL, *where_col2_tok = NULL;
	int rel_op1 = 0, rel_op2 = 0; //holds type of relational operators in where clauses
	token_list *rel_value = NULL, *rel_value2 = NULL; //holds value of comparison item in where clauses

	cur = t_list; //parse the token list
	if ((cur->tok_value != S_STAR) && (cur->tok_class != identifier) && (cur->tok_class != function_name))
	{	// Error
		printf("invalid select statement\n");
		rc = INVALID_SELECT_STATEMENT;
		cur->tok_value = INVALID;
	}
	else {	//select stmt has *, identifer or aggregate func
		if(cur->tok_class == identifier){//search by column
			token_list *columns = cur;
			while(cur->tok_value != K_FROM)
				cur = cur->next; //move past column names

			if(cur->tok_value != EOC){//from keyword is present
				cur = cur->next; //move past from to get tablename
			}
			else{
				printf("missing 'from' keyword in select statement\n");
				rc = INVALID_SELECT_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}

			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL){
				printf("cannot select from nonexistent table\n");
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{	//get columns to use for printing buffer
				while(columns->tok_value != K_FROM){
					if(columns->tok_value == EOC){
						printf("Invalid select statement\n");
						rc = INVALID_SELECT_BY_COLUMN_STATEMENT;
						columns->tok_value = INVALID;
					}
					else{//columnfinder searchs tpd for matching col and returns col # if found
						int col_found = columnFinder(tab_entry, columns->tok_string);
						if(col_found > -1){
							colArray[index] = col_found; //add to array of column ids
							index++;
							if(columns->next->tok_value == S_COMMA)
								columns = columns->next->next;
							else
								break;
						}
						else{
							printf("column not found in table %s\n", tab_entry->table_name);
							rc = COLUMN_NOT_EXIST;
							columns->tok_value = INVALID;
							return rc;
						}
					}
				}//end while for columns
			}
		}//select by column
		else if(cur->tok_value == S_STAR){//for select *
			cur = cur->next;
			if(cur->tok_value != K_FROM){
				printf("missing 'from' keyword in select statement\n");
				rc = INVALID_SELECT_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}
			else{
				cur = cur->next;
				if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL){
					printf("cannot select from nonexistent table\n");
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}//else - parse OK, continue
			}
		}//end select *
		else if (cur->tok_class == function_name){ //for aggregate functions
			token_list *agg = cur;
			while(cur->tok_value != K_FROM)
				cur = cur->next; //move past aggegreate func

			if(cur->tok_value != EOC){ //from keyword is present
				cur = cur->next; //move past from to get table
			}
			else{
				printf("missing 'from' keyword in select statement\n");
				rc = INVALID_SELECT_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}

			if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
			{
				printf("cannot select from nonexistent table\n");
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}
			else
			{	//41-sum, 42-avg, 43-count
				aggregate_func = agg->tok_value;
				aggregate_col_tok = agg->next->next;
				rc = checkAggregateSyntax(tab_entry, agg);
				if(rc == 300){
					aggregate_col = 300; //for count(*)
					rc = 0;
				}
				else if(rc >= 0){//not error
					aggregate_col = rc;
					rc = 0;
				}
			}
		}//select aggregate
	}//end else

	if(!rc){//stmt parse ok - cur should be at table name in both cases
		table_info = getTFH(tab_entry);
		int num_records = table_info->num_records;
		int record_size = table_info->record_size;
		the_buffer = get_buffer(tab_entry);
		cur = cur->next;

		if(cur->tok_value == K_WHERE){
			cur = cur->next;//move past where
			//FIRST WHERE CLAUSE
			int where_col1 = columnFinder(tab_entry, cur->tok_string);
			if(where_col1 > -1){
				where_col1_tok = cur; //store the col # for first where clause
				cur = cur->next;
				if((cur->tok_value == S_EQUAL) || (cur->tok_value == S_LESS) 
						|| (cur->tok_value == S_GREATER) || (cur->tok_value == K_IS))
				{
					rel_op1 = cur->tok_value;
					if(cur->tok_value == K_IS){
						if(cur->next->tok_value == K_NOT){
							rel_op1 = cur->next->tok_value; //set rel_op1 to NOT
							cur = cur->next; //cur is at NOT keyword now
						}

						if(cur->next->tok_value != K_NULL){
							printf("invalid use of the keyword 'is' which can only be followed by 'null' or 'not null'\n");
							rc = INVALID_WHERE_CLAUSE_IN_SELECT;
							cur->next->tok_value = INVALID;
							return rc;
						}
					}
					else{
						if( (cur->next->tok_value != INT_LITERAL) || (cur->next->tok_value != STRING_LITERAL)){
							printf("invalid relational operator\n");
							rc = INVALID_REL_OP_IN_WHERE_OF_SELECT;
							cur->next->tok_value = INVALID;
							return rc;
						}
						else{//checkcoltype returns 1 if the col type matches and negative # when error
							int type_match = checkColType(tab_entry, cur->next->tok_string, cur->next->tok_value, where_col1);
							if(type_match != 1){
								printf("data type in where statement does not match column type (too long/too large)\n");
								rc = MISMATCH_TYPE_IN_WHERE_OF_SELECT;
								cur->next->tok_value = INVALID;
								return rc;
							}
						}
					}

					//check for data_value
					if(cur->next->tok_value != EOC){
						cur = cur->next;
						rel_value = cur; //set equal to rel value (number, char, or 'null')
						cur = cur->next; //first where condition parse OK
					}
					else{
						printf("invalid where clause. missing data value\n");
						rc = INVALID_WHERE_CLAUSE_IN_SELECT;
						cur->next->tok_value = INVALID;
						return rc;
					}
				}
				else{
					printf("invalid or missing relational operator in where clause\n");
					rc = INVALID_REL_OP_IN_WHERE_OF_SELECT;
					cur->tok_value = INVALID;
					return rc;
				}
			}//first column in first where clause found
			else{
				printf("column not found in table %s\n", tab_entry->table_name);
				rc = TABLE_NOT_EXIST;
				cur->tok_value = INVALID;
			}

			filtered_buffer = selectRowsForValue(the_buffer, tab_entry, where_col1, rel_value, rel_op1, num_records, record_size);
			int matches = getNumberOfMatches(the_buffer, tab_entry, where_col1, rel_value, rel_op1, num_records, record_size);

			//AFTER FIRST WHERE CLAUSE
			if(cur->tok_value == EOC){
				if(index > 0){ //index is a counter for colArray - tells which col to select
					rc = print_select_from_buffer(tab_entry, filtered_buffer, matches*record_size, matches, record_size, colArray, index);
				}
				else if(aggregate_func){//print aggregate func
					rc = print_aggregate(tab_entry, table_info, filtered_buffer, aggregate_col, aggregate_col_tok, aggregate_func, matches);
				}
				else{//prints select *
					rc = print_select_from_buffer(tab_entry, filtered_buffer, matches*record_size, matches, record_size);
				}
			}
			else if( (cur->tok_value == K_AND) || (cur->tok_value == K_OR)){
				int conjunction = cur->tok_value; //conjunction is either 'and' or 'or'
				if(cur->next->tok_value != EOC){
					//PARSE SECOND WHERE CLAUSE
					cur = cur->next; //move past conjunction
					int where_col2 = columnFinder(tab_entry, cur->tok_string); //find col for 2nd where clause
					if(where_col2 > -1){
						where_col2_tok = cur;
						cur = cur->next;

						if((cur->tok_value == S_EQUAL) || (cur->tok_value == S_LESS) 
								|| (cur->tok_value == S_GREATER) || (cur->tok_value == K_IS))
						{
							rel_op2 = cur->tok_value;
							if(cur->tok_value == K_IS){
								if(cur->next->tok_value == K_NOT){
									rel_op2 = cur->next->tok_value; //set rel_op2 to NOT
									cur = cur->next; //cur @ NOT now
								}

								if(cur->next->tok_value != K_NULL){
									printf("invalid use of the keyword 'is' which can only be followed by 'null' or 'not null'\n");
									rc = INVALID_WHERE_CLAUSE_IN_SELECT;
									cur->next->tok_value = INVALID;
									return rc;
								}
							}
							else{
								if( (cur->next->tok_value != INT_LITERAL) || (cur->next->tok_value != STRING_LITERAL)){
									printf("invalid relational operator\n");
									rc = INVALID_REL_OP_IN_WHERE_OF_SELECT;
									cur->next->tok_value = INVALID;
									return rc;
								}
								else{
									int type_match = checkColType(tab_entry, cur->next->tok_string, cur->next->tok_value, where_col2);
									if(type_match != 1){
										printf("data type in where statement does not match column type (too long/too large)\n");
										rc = MISMATCH_TYPE_IN_WHERE_OF_SELECT;
										cur->next->tok_value = INVALID;
										return rc;
									}
								}
							}

							//check for data_value
							if(cur->next->tok_value != EOC){
								cur = cur->next;
								rel_value2 = cur; //set equal to rel value (number, char, or 'null')
								
								cur = cur->next; //first where condition parse OK
								//printf("first where stmt parse OK - todo: filter\n");
							}
							else{
								printf("invalid where clause. missing data value\n");
								rc = INVALID_WHERE_CLAUSE_IN_SELECT;
								cur->next->tok_value = INVALID;
								return rc;
							}
						}
						else{
							printf("invalid or missing relational operator in where clause\n");
							rc = INVALID_REL_OP_IN_WHERE_OF_SELECT;
							cur->tok_value = INVALID;
							return rc;
						}
					}//first column in second where clause found
					else{
						printf("column not found in table %s\n", tab_entry->table_name);
						rc = TABLE_NOT_EXIST;
						cur->tok_value = INVALID;
					}

					int matches2 = 0;
					switch(conjunction){
						case 34: //and
							second_filter_buffer = selectRowsForValue(filtered_buffer, tab_entry, where_col2, rel_value2, rel_op2, matches, record_size);
							matches2 = getNumberOfMatches(filtered_buffer, tab_entry, where_col2, rel_value2, rel_op2, matches, record_size);
							break;
						case 35: //or
							second_filter_buffer = selectRowsForValueOr(the_buffer, tab_entry, where_col1, rel_value, rel_op1, num_records, record_size, where_col2, rel_value2, rel_op2);
							matches2 = getNumberOfMatchesOr(the_buffer, tab_entry, where_col1, rel_value, rel_op1, num_records, record_size, where_col2, rel_value2, rel_op2);
							break;
					}

					//AFTER SECOND WHERE CLAUSE	
					if(cur->tok_value == EOC){
						if(index > 0){
							rc = print_select_from_buffer(tab_entry, second_filter_buffer, matches2*record_size, matches2, record_size, colArray, index);
						}
						else if(aggregate_func){
							rc = print_aggregate(tab_entry, table_info, second_filter_buffer, aggregate_col, aggregate_col_tok, aggregate_func, matches2);
						}
						else{
							rc = print_select_from_buffer(tab_entry, second_filter_buffer, matches2*record_size, matches2, record_size);
						}
					}
					else if (cur->tok_value == K_ORDER){
						if(cur->next->tok_value == K_BY){
							cur = cur->next->next; //move past 'order by'
							int order_col = columnFinder(tab_entry, cur->tok_string); //find col to order by
							if(order_col > -1){
								cur = cur->next;
								if(cur->tok_value == EOC){//default is ascending order
									ordered_buffer = orderByBuffer(second_filter_buffer, tab_entry, order_col, 1, record_size, matches2);
									if(index > 0){//columns were given
										rc = print_select_from_buffer(tab_entry, second_filter_buffer, matches2*record_size, matches2, record_size, colArray, index);
									}
									else if(aggregate_func){
										printf("aggregate function ignores order by clause\n");
										rc = print_aggregate(tab_entry, table_info, second_filter_buffer, aggregate_col, aggregate_col_tok, aggregate_func, matches2);
									}
									else{
										rc = print_select_from_buffer(tab_entry, second_filter_buffer, matches2*record_size, matches2, record_size);
									}
								}
								else if (cur->tok_value == K_DESC){//add desc for descending order
									ordered_buffer = orderByBuffer(second_filter_buffer, tab_entry, order_col, K_DESC, record_size, matches2);
									if(index > 0){
										//columns were given
										rc = print_select_from_buffer(tab_entry, second_filter_buffer, matches2*record_size, matches2, record_size, colArray, index);
									}
									else if(aggregate_func){
										printf("aggregate function ignores order by clause\n");
										rc = print_aggregate(tab_entry, table_info, second_filter_buffer, aggregate_col, aggregate_col_tok, aggregate_func, matches2);
									}
									else{
										rc = print_select_from_buffer(tab_entry, second_filter_buffer, matches2*record_size, matches2, record_size);
									}
								}
								else{
									printf("invalid symbol or keyword in order by clause of select statement\n");
									rc = INVALID_ORDER_BY_CLAUSE_IN_SELECT;
									cur->tok_value = INVALID;
								}
							}
							else{
								printf("order by column not found in table %s\n", tab_entry->table_name);
								rc = COLUMN_NOT_EXIST;
								cur->tok_value = INVALID;
							}
						}
						else {
							printf("syntax error: keyword 'by' must follow keyword 'order'\n");
							cur->next->tok_value = INVALID;
							rc = INVALID_ORDER_BY_CLAUSE_IN_SELECT;
							return rc;
						}
					}
					else{
						printf("unexpected keyword or symbol after 2nd where clause of select statement\n");
						rc = UNEXPECTED_AFTER_WHERE_OF_SELECT;
						cur->tok_value = INVALID;
						return rc;
					}
				}
				else{
					printf("a clause must come after the conjunction %s\n",cur->tok_string);
					rc = INVALID_SELECT_STATEMENT;
					cur->tok_value = INVALID;
					return rc;
				}
			}
			else if (cur->tok_value == K_ORDER){ //no 2nd where clause - but has order by clause
				if(cur->next->tok_value == K_BY){
					cur = cur->next->next; //move past 'order by'
					int order_col = columnFinder(tab_entry, cur->tok_string);
					if(order_col > -1){
						cur = cur->next;
						if(cur->tok_value == EOC){
							ordered_buffer = orderByBuffer(filtered_buffer, tab_entry, order_col, 1, record_size, matches);

							if(index > 0){
								//columns were given
								rc = print_select_from_buffer(tab_entry, ordered_buffer, matches*record_size, matches, record_size, colArray, index);
							}
							else if(aggregate_func){
								printf("aggregate function ignores order by clause\n");
								rc = print_aggregate(tab_entry, table_info, ordered_buffer, aggregate_col, aggregate_col_tok, aggregate_func, matches);
							}
							else{
								rc = print_select_from_buffer(tab_entry, ordered_buffer, matches*record_size, matches, record_size);
							}
						}
						else if (cur->tok_value == K_DESC){
							ordered_buffer = orderByBuffer(filtered_buffer, tab_entry, order_col, K_DESC, record_size, matches);

							if(index > 0){
								//columns were given
								rc = print_select_from_buffer(tab_entry, ordered_buffer, matches*record_size, matches, record_size, colArray, index);
							}
							else if(aggregate_func){
								printf("aggregate function ignores order by clause\n");
								rc = print_aggregate(tab_entry, table_info, ordered_buffer, aggregate_col, aggregate_col_tok, aggregate_func, matches);
							}
							else{
								rc = print_select_from_buffer(tab_entry, ordered_buffer, matches*record_size, matches, record_size);
							}
						}
						else{
							printf("invalid symbol or keyword in order by clause of select statement\n");
							rc = INVALID_ORDER_BY_CLAUSE_IN_SELECT;
							cur->tok_value = INVALID;
						}
					}
					else{
						printf("order by column not found in table %s\n", tab_entry->table_name);
						rc = COLUMN_NOT_EXIST;
						cur->tok_value = INVALID;
					}
				}
				else {
					printf("syntax error: keyword 'by' must follow keyword 'order'\n");
					cur->next->tok_value = INVALID;
					rc = INVALID_ORDER_BY_CLAUSE_IN_SELECT;
					return rc;
				}
			}
			else{
				printf("unexpected keyword or symbol after where clause of select statement\n");
				rc = UNEXPECTED_AFTER_WHERE_OF_SELECT;
				cur->tok_value = INVALID;
				return rc;
			}
		}
		else if (cur->tok_value == K_ORDER){//no where clause, but has order by clause
			if(cur->next->tok_value == K_BY){
				cur = cur->next->next; //move past 'order by'
				int order_col = columnFinder(tab_entry, cur->tok_string);
				if(order_col > -1){
					cur = cur->next;
					if(cur->tok_value == EOC){
						ordered_buffer = orderByBuffer(the_buffer, tab_entry, order_col, 1, table_info->record_size, table_info->num_records);
						if(index > 0){
							//columns were given
							rc = print_select_from_buffer(tab_entry, ordered_buffer, table_info->num_records*table_info->record_size, table_info->num_records, table_info->record_size, colArray, index);
						}
						else if(aggregate_func){
							printf("aggregate function ignores order by clause\n");
							rc = print_aggregate(tab_entry, table_info, the_buffer, aggregate_col, aggregate_col_tok, aggregate_func, table_info->num_records);
						}
						else{
							rc = print_select_from_buffer(tab_entry, ordered_buffer, table_info->num_records*table_info->record_size, table_info->num_records, table_info->record_size);
						}
					}
					else if (cur->tok_value == K_DESC){
						ordered_buffer = orderByBuffer(the_buffer, tab_entry, order_col, K_DESC, table_info->record_size, table_info->num_records);

						if(index > 0){
							//columns were given
							rc = print_select_from_buffer(tab_entry, ordered_buffer, table_info->num_records*table_info->record_size, table_info->num_records, table_info->record_size, colArray, index);
						}
						else if(aggregate_func){
							printf("aggregate function ignores order by clause\n");
							rc = print_aggregate(tab_entry, table_info, the_buffer, aggregate_col, aggregate_col_tok, aggregate_func, table_info->num_records);
						}
						else{
							rc = print_select_from_buffer(tab_entry, ordered_buffer, table_info->num_records*table_info->record_size, table_info->num_records, table_info->record_size);
						}
					}
					else{
						printf("invalid symbol or keyword in order by clause of select statement\n");
						rc = INVALID_ORDER_BY_CLAUSE_IN_SELECT;
						cur->tok_value = INVALID;
					}
				}
				else{
					printf("order by column not found in table %s\n", tab_entry->table_name);
					rc = COLUMN_NOT_EXIST;
					cur->tok_value = INVALID;
				}
			}
			else {
				printf("syntax error: keyword 'by' must follow keyword 'order'\n");
				cur->next->tok_value = INVALID;
				rc = INVALID_ORDER_BY_CLAUSE_IN_SELECT;
				return rc;
			}
		}
		else if (cur->tok_value == EOC){ //no where or order by clause
			if(index > 0){
				//columns were given
				rc = print_select(tab_entry, colArray, index);
			}
			else if(aggregate_func){
				rc = print_aggregate(tab_entry, table_info, the_buffer, aggregate_col, aggregate_col_tok, aggregate_func, num_records);
			}
			else{
				rc = print_selectAll(tab_entry);
			}
		}
		else{
			printf("syntax error: unexpected symbol or keyword in select statement\n");
			rc = INVALID_SELECT_STATEMENT;
			cur->tok_value = INVALID;
		}
	}
	return rc;
}

int sem_delete(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	table_file_header *recs = NULL;

	token_list *delete_token = NULL;	//token for delete
	int where_col_no; 					//column in where condition
	int rel_op; 						//relational operator in where condition
	bool has_where = false;				//flag to perform where check or not

	cur = t_list;
	cur = cur->next; //move past FROM keyword
	if ((cur->tok_class != keyword) && (cur->tok_class != identifier) &&
			(cur->tok_class != type_name)) { 	//Error
		printf("invalid delete statement\n");
		rc = INVALID_DELETE_STATEMENT;
		cur->tok_value = INVALID;
	}
	else{ 	//identifier found - check that table exists
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL){
			printf("cannot delete from nonexistent table\n");
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;
			if(cur->tok_value != EOC){
				if(cur->tok_value == K_WHERE){
					has_where = true;
					if(cur->next->tok_value != EOC){
						cur = cur->next;
						where_col_no = columnFinder(tab_entry, cur->tok_string);//get column names
						if(where_col_no > -1){
							cur = cur->next;
							if((cur->tok_value == S_EQUAL) || (cur->tok_value == S_LESS) || (cur->tok_value == S_GREATER))
							{
								rel_op = cur->tok_value;
								cur = cur->next; //move past relational operator
								if((cur->tok_value != INT_LITERAL) && (cur->tok_value != STRING_LITERAL) && (cur->tok_value != K_NULL))
								{
									printf("invalid type value in where clause. only string literals, int literals, and 'null' are supported.\n");
									rc = INVALID_TYPE_IN_WHERE_OF_DELETE;
									cur->tok_value = INVALID;
									return rc;
								}
								else 
								{	//check that data type specified in where matches col
									int where_match = checkColType(tab_entry, cur->tok_string, cur->tok_value, where_col_no);//returns -1 for type mismatch, 1 or 0 for proper size of char and int
									if(where_match != 1){ //not a type match
										printf("type in where statement does not match column type\n");
										rc = MISMATCH_TYPE_IN_WHERE_OF_DELETE;
										cur->tok_value = INVALID;
										return rc;
									}
									else{
										if(cur->next->tok_value != EOC){
											printf("invalid clause after where clause of update\n");
											rc = INVALID_UPDATE_STATEMENT;
											cur->next->tok_value = INVALID;
										}
										//else - parse ok
									}//match ok in where clause
								}//valid type in where clause
							}
							else
							{
								printf("invalid relational operator found. only <, =, and > are supported\n");
								rc = INVALID_REL_COMP_IN_WHERE_IN_DELETE;
								cur->tok_value = INVALID;
							}
						}//column in where found in table
						else{
							printf("no matching column in table from where clause to select for deletion\n");
							rc = INVALID_COLUMN_NAME;
							cur->tok_value = INVALID;
						}
					}//tokens after where found
					else{
						printf("improper use of the keyword 'where'. a clause must follow\n");
						rc = INVALID_WHERE_CLAUSE_IN_DELETE;
						cur->tok_value = INVALID;
					}
				}//where keyword found
				else{
					printf("invalid delete statement after table name\n");
					rc = INVALID_DELETE_STATEMENT;
					cur->tok_value = INVALID;
				}
			}
		}//end else for table exists
	}//end else for identifier found

	if(!rc)
	{
		if(has_where){//has where flag set in parse above
			int to_delete = checkRowsForDelete(tab_entry, rel_op, cur, where_col_no); //check for matching value in the specified col
			switch(to_delete)
			{
				case -1:
					rc = FILE_OPEN_ERROR;
					break;
				case -2:
					rc = MEMORY_ERROR;
					break;
				case -3:
					rc = DBFILE_CORRUPTION;
					break;
				case 0:
					printf("0 rows deleted.\n");
					rc = NO_MATCHING_ROWS_TO_DELETE;
					//cur->tok_value = INVALID;
					break;
			}//printf("number rows matching to delete is %d\n", to_delete);
		}
		else
		{
			int delete_item = deleteHelper(tab_entry->table_name);
			switch(delete_item)
			{
				case -1:
					rc = FILE_OPEN_ERROR;
					break;
				case -2:
					rc = MEMORY_ERROR;
					break;
			}
		}
	}
	return rc;
}

int sem_update(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	struct _stat file_stat;
	table_file_header *recs = NULL;
	
	//for update stmt
	int column_num; 					//column to update
	token_list *update_token = NULL;	//token for update
	int where_col_no; 					//column in where condition
	int rel_op; 						//relational operator in where condition
	bool has_where = false;				//flag to perform where check or not
	
	cur = t_list;
	if ((cur->tok_class != keyword) && (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{ 	//Error
		printf("invalid update statement\n");
		rc = INVALID_UPDATE_STATEMENT;
		cur->tok_value = INVALID;
		return rc;
	}
	else
	{	//identifier found - check that table exists
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
		{
			printf("cannot update nonexistent table\n");
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
			return rc;
		}//table does not exist
		else
		{
			cur = cur->next;
			if(cur->tok_value == K_SET)
			{
				cur = cur->next; //move to column name
				if(cur->tok_value != EOC)
				{	//retrieve column number of col to update
					column_num = columnFinder(tab_entry, cur->tok_string);
					if(column_num > -1)
					{
						cur = cur->next;
						if(cur->tok_value != S_EQUAL)
						{
							printf("invalid update statement. must set a column = to a value\n");
							rc = INVALID_REL_OP_IN_UPDATE;
							cur->tok_value = INVALID;
						}
						else
						{
							cur = cur->next;
							if((cur->tok_value != INT_LITERAL) && (cur->tok_value != STRING_LITERAL) && (cur->tok_value != K_NULL))
							{
								printf("invalid update value. only string literals, int literals, and 'null' are supported.\n");
								rc = INVALID_UPDATE_TYPE;
								cur->tok_value = INVALID;
								return rc;
							}
							else 
							{
								int type_match = checkColType(tab_entry, cur->tok_string, cur->tok_value, column_num);
								//returns -1 for type mismatch, 1 or 0 for proper size of char and int
								if(type_match != 1)
								{ //not a type match
									switch(type_match)
									{
										case -2:
											printf("not null constraint\n");
											rc = UPDATE_NOT_NULL_CONSTRAINT;
											break;
										case -1:
											printf("update value does not match column type\n");
											rc = UPDATE_TYPE_MISMATCH;
											break;
										case 0:
											if(cur->tok_value == INT_LITERAL)
											{
												printf("integer size error\n");
												rc = UPDATE_INT_SIZE_FAILURE;
											}
											else if(cur->tok_value == STRING_LITERAL)
											{
												printf("update char length error\n");
												rc = UPDATE_CHAR_LEN_MISMATCH;
											}
											break;
									}
									cur->tok_value = INVALID;
									return rc;
								}
								else
								{
									update_token = cur; //pointer to token for use in update
									cur = cur->next;
									if(cur->tok_value != EOC)
									{	
										if (cur->tok_value == K_WHERE)
										{
											has_where = true;
											cur = cur->next;
											where_col_no = columnFinder(tab_entry, cur->tok_string);
											if(where_col_no > -1)
											{
												cur = cur->next;
												if((cur->tok_value == S_EQUAL) || (cur->tok_value == S_LESS) || (cur->tok_value == S_GREATER))
												{
													rel_op = cur->tok_value;
													cur = cur->next; //move past relational operator
													if((cur->tok_value != INT_LITERAL) && (cur->tok_value != STRING_LITERAL) && (cur->tok_value != K_NULL))
													{
														printf("invalid type value in where clause. only string literals, int literals, and 'null' are supported.\n");
														rc = INVALID_TYPE_IN_WHERE_OF_UPDATE;
														cur->tok_value = INVALID;
														return rc;
													}
													else 
													{
														int where_match = checkColType(tab_entry, cur->tok_string, cur->tok_value, where_col_no);
														//returns -1 for type mismatch, 1 or 0 for proper size of char and int
														if(where_match != 1)
														{ //not a type match
															switch(where_match)
															{
																case -2: //null in where clause but column cannot be null'ed	
																	printf("column in where cannot be set to null, so no matching rows to update\n");
																	rc = UPDATE_NOT_NULL_CONSTRAINT;
																	break;
																case -1:
																	printf("type in where statement does not match column type\n");
																	rc = MISMATCH_TYPE_IN_WHERE_OF_UPDATE;
																	break;
																case 0:
																	if(cur->tok_value == INT_LITERAL)
																	{
																		printf("integer size error\n");
																		rc = UPDATE_INT_SIZE_FAILURE;
																	}
																	else if(cur->tok_value == STRING_LITERAL)
																	{
																		printf("update char length error\n");
																		rc = UPDATE_CHAR_LEN_MISMATCH;
																	}
																	break;
															}
															cur->tok_value = INVALID;
															return rc;
														}
														else
														{
															if(cur->next->tok_value != EOC)
															{
																printf("invalid clause after where clause of update\n");
																rc = INVALID_UPDATE_STATEMENT;
																cur->next->tok_value = INVALID;
															}
															//else - parse ok
														}//match ok in where clause
													}//data value in where clause is right type
												}//valid relational operator
												else
												{
													printf("invalid relational operator found. only <, =, and > are supported\n");
													rc = INVALID_REL_OP_IN_UPDATE;
													cur->tok_value = INVALID;
												}
											}//column name in where clause found
											else
											{
												printf("no matching column in where clause of update statement\n");
												rc = NO_MATCHING_COL_IN_WHERE_OF_UPDATE;
												cur->tok_value = INVALID;
											}
										}
										else
										{
											printf("invalid update statement after set clause\n");
											rc = INVALID_UPDATE_STATEMENT;
											cur->tok_value = INVALID;
										}
									}//there is a where clause
									//else - parse ok
								}
							}//valid input value type
						}//equal sign found
					}//column name found
					else
					{
						printf("no matching column to update\n");
						rc = NO_MATCHING_COL_TO_UPDATE;
						cur->tok_value = INVALID;
						return rc;
					}
				}//values after set keyword
				else
				{
					printf("invalid update statement. missing column to set\n");
					rc = INVALID_UPDATE_STATEMENT;
					cur->tok_value = INVALID;
					return rc;
				}//missing column name
			}//missing 'set' keyword
			else 
			{
				printf("invalid update statement. missing set clause\n");
				rc = INVALID_UPDATE_STATEMENT;
				cur->tok_value = INVALID;
				return rc;
			}//missing set keyword
		}//end else for table exists
	}//end parsing

	if (!rc)
	{
		if(has_where)
		{
			int ok_to_update = checkRowsForValue(tab_entry, column_num, update_token, rel_op, cur, where_col_no);
			switch(ok_to_update)
			{
				case -3:
					rc = DBFILE_CORRUPTION;
					break;
				case -2:
					rc = MEMORY_ERROR;
					break;
				case -1:
					rc = FILE_OPEN_ERROR;
					break;
				case 0:
					//printf("0 rows updated.\n");
					rc = NO_MATCHING_ROW_TO_UPDATE;
					//cur->tok_value = INVALID;
					break;
				default:
					;//printf("match found"); //match found - OK to update
			}
		}//has where clause
		else
		{	//no where clause, update all rows
			int update = updateHelper(tab_entry, column_num, update_token);
			switch(update)
			{
				case -1:
					rc = FILE_OPEN_ERROR;
					break;
				case -2:
					rc = MEMORY_ERROR;
					break;
			}
		}//no where clause, update all rows
	}

	return rc;
}

int sem_backup(token_list *t_list)
{
	int rc = 0;
	token_list *cur = t_list;
	FILE *fhandle = NULL;
	FILE *fbackup = NULL;
	struct _stat file_stat;
	int backup_file_size = 0;

	if(cur->tok_class == identifier){
		if(cur->next->tok_value == EOC){
			if((fbackup = fopen(cur->tok_string, "rbc")) == NULL)
			{
				if((fbackup = fopen(cur->tok_string, "wbc")) == NULL)
				{
					rc = FILE_OPEN_ERROR;
				}
				else
				{	//Backup dbfile.bin
					if((fhandle = fopen("dbfile.bin", "rbc")) != NULL)
					{
						_fstat(_fileno(fhandle), &file_stat);
						printf("backing up dbfile.bin = %d\n",file_stat.st_size);
						backup_file_size += file_stat.st_size + 4;
						tpd_entry *dbfile = (tpd_entry*)calloc(1, file_stat.st_size);

						if(!dbfile)
						{
							rc = MEMORY_ERROR;
							return rc;
						}
						else
						{	//read db.bin
							fread(dbfile, file_stat.st_size, 1, fhandle);
							fflush(fhandle);
							fwrite(&file_stat.st_size, 4, 1, fbackup); //write db.bin to bkup file
							fwrite(dbfile, file_stat.st_size, 1, fbackup);
						}
					}
					else{
						printf("error opening or dbfile.bin does not exist\n");
						rc = FILE_OPEN_ERROR;
						return rc;
					}

					//Backup each .tab file in order
					int num_tables = g_tpd_list->num_tables;
					tpd_entry *cur_tab = &(g_tpd_list->tpd_start);
					if(num_tables>0)
					{
						while (num_tables-- > 0)
						{
							char *tablename = (char*)calloc(1,strlen(cur_tab->table_name)+4);
							strcat(tablename, cur_tab->table_name); //get table name to backup
							strcat(tablename, ".tab"); 
							if((fhandle = fopen(tablename, "rbc")) != NULL)
							{
								_fstat(_fileno(fhandle), &file_stat);
								printf("backing up %s.tab = %d\n", cur_tab->table_name, file_stat.st_size);
								backup_file_size += file_stat.st_size + 4;
								table_file_header *table = (table_file_header*)calloc(1, file_stat.st_size);

								if(!table)
								{
									rc = MEMORY_ERROR;
									return rc;
								}
								else
								{
									fread(table, file_stat.st_size, 1, fhandle);//read contents of table.tab
									fflush(fhandle);
									fwrite(&file_stat.st_size, 4, 1, fbackup); //write table.tab to bkup file
									fwrite(table, file_stat.st_size, 1, fbackup);
								}
							}
							else
							{
								printf("error opening or %s.tab does not exist\n", cur_tab->table_name);
								rc = FILE_OPEN_ERROR;
								return rc;
							}

							if (num_tables > 0)
							{
								cur_tab = (tpd_entry*)((char*)cur_tab + cur_tab->tpd_size);
							}
						}
					}
					else{
						printf("no tables to backup\n");
					}
					printf("%s size = %d\n", cur->tok_string, backup_file_size);
				}
			}
			else
			{
				printf("img file with the name %s already exists. backup failure.\n", cur->tok_string);
				rc = DUPLICATE_IMG_FILENAME;
				cur->tok_value = INVALID;
			}
		}
		else{
			printf("unexpected symbol after backup statement\n");
			rc = INVALID_BACKUP_STATEMENT;
			cur->next->tok_value = INVALID;
		}
	}
	else{
		printf("image file name must be given to backup to\n");
		rc = MISSING_IMG_FILENAME;
		cur->tok_value = INVALID;
	}

	return rc;
}

int sem_restore(token_list *t_list)
{
	int rc = 0;
	token_list *cur = t_list;
	FILE *fhandle = NULL;
	struct _stat file_stat;
	bool without_rf = false;

	if(cur->tok_class == identifier)
	{	//get image file identifier
		token_list *img_file_name = cur;
		if(cur->next->tok_value == EOC){
			//printf("restore w/o RF\n"); parse OK
		}
		else if (cur->next->tok_value == K_WITHOUT){
			cur = cur->next->next;
			if(cur->tok_value == K_RF){
				without_rf = true;
			}
			else{
				printf("without keyword can only be followed by 'rf'\n");
				rc = INVALID_RESTORE_STATEMENT;
				cur->tok_value = INVALID;
			}
		}
		else{
			printf("unexpected symbol found in restore statement\n");
			rc = INVALID_RESTORE_STATEMENT;
			cur->next->tok_value = INVALID;
		}

		if(!rc)
		{
			if((fhandle = fopen(img_file_name->tok_string, "rbc")) == NULL)
			{
				printf("%s image file does not exist. cannot restore from it\n", img_file_name->tok_string);
				rc = FILE_OPEN_ERROR;
				img_file_name->tok_value = INVALID;
			}
			else
			{
				rc = restoreHelper(img_file_name); //call helper func to do the restore
				rc = pruneLog(img_file_name, without_rf); //prune the log as necessary for w/ or w/o rf
			}
		}
	}
	else
	{
		printf("image file name must be given to restore from\n");
		rc = MISSING_IMG_FILENAME;
		cur->tok_value = INVALID;
	}

	return rc;
}

int sem_rollforward(token_list *t_list)
{
	int rc = 0;
	token_list *cur = t_list;
	char *now; //for current ts
	FILE *fhandle = NULL;
	struct _stat file_stat;
	char *existing_log = NULL, *redo = NULL, *prune_redo = NULL;
	int offset = 0;

	if( (fhandle = fopen("db.log","r")) != NULL){
		_fstat(_fileno(fhandle), &file_stat);
		existing_log = (char*)calloc(1, file_stat.st_size);
		char ts[16];
		char *cmd = (char*)calloc(1, MAX_PRINT_LEN);
		char *bkup = (char*)calloc(1, MAX_PRINT_LEN);
		char *toCompare = (char*)calloc(1, 10);
		strcat(toCompare, "\"RF_START\"");
		bool found_rf_start = false;
		
		while(!feof(fhandle))
		{
			if(fgets(ts,16,fhandle) != NULL){
				//printf("ts is %s, ", ts);
			}
			if(fgets(cmd,MAX_PRINT_LEN,fhandle) != NULL){
				if(strncmp(toCompare, cmd, strlen(toCompare)) == 0){//if rf_start is found in log
					offset = ftell(fhandle);//get the offset of the rf_start in log
					found_rf_start = true;
					break;
				}
				else{
					strcat(existing_log,ts);
					strcat(existing_log, cmd);
				}
			}
		}//get existing log before rf_start

		redo = (char*)calloc(1,file_stat.st_size - offset);
		while(!feof(fhandle)){
			if(fgets(bkup,16,fhandle) != NULL){
				strcat(redo,bkup);
			}
			if(fgets(bkup,MAX_PRINT_LEN,fhandle) != NULL){
				strcat(redo,bkup);
			}
		}
		fflush(fhandle);
		fclose(fhandle);

		if(!found_rf_start){
			printf("no restore point found. please use restore command before roll forward\n");
			rc = MISSING_RF_START_IN_LOG;
			return rc;
		}
		else{//reset flag in g_tpd_list
			g_tpd_list->db_flags = 0;
			if((fhandle= fopen("dbfile.bin", "rbc")) != NULL)
			{
				if((fhandle = fopen("dbfile.bin", "wbc")) != NULL)
				{
					fwrite(g_tpd_list, g_tpd_list->list_size, 1, fhandle);
					fflush(fhandle);
					fclose(fhandle);
				}
				else{
					rc = FILE_OPEN_ERROR;
				}
			}
			else{
				rc = FILE_OPEN_ERROR;
			}
		}
	}
	else{
		printf("error opening db.log\n");
		rc = FILE_OPEN_ERROR;
	}

	if(!rc){
		if(cur->tok_value == EOC){
			printf("need to redo all commands\n");

			if( (fhandle = fopen("db.log","r")) != NULL){
				_fstat(_fileno(fhandle), &file_stat);
				//existing_log = (char*)calloc(1, file_stat.st_size);
				char ts[16];
				char *cmd = (char*)calloc(1, MAX_PRINT_LEN);
				char *bkup = (char*)calloc(1, MAX_PRINT_LEN);
				char *toCompare = (char*)calloc(1, 10);
				//strcat(toCompare, "\"RF_START\"");
				//bool found_rf_start = false;
				
				fseek(fhandle, offset, SEEK_SET);
				//redo = (char*)calloc(1,file_stat.st_size - offset);
				while(!feof(fhandle)){
					if(fgets(ts,16,fhandle) != NULL){
						//strcat(redo,bkup);
						//printf("ts is %s, ", ts);
					}

					fseek(fhandle, 1, SEEK_CUR);//skip first quote
					if(fgets(bkup,MAX_PRINT_LEN,fhandle) != NULL){
						//strcat(redo,bkup);
						//printf("bkup is '%s' and length is %d\n",bkup,strlen(bkup)-2);
						//remove endline character and last quote
						char *cmd = (char*)calloc(1,strlen(bkup)-1); //need one extra for null terminator
						strncpy(cmd, bkup, strlen(bkup)-2);
						printf("rolling forward: \"%s\"\n", cmd);
						//printf("%d\n", strlen(cmd));

						token_list *t_list = NULL;
						rc = get_token(cmd, &t_list);
						//printf("rc is %d\n", rc);
						if(!rc){
							if(t_list->tok_value != K_BACKUP){//skip redoing of backup steps
								rc = do_semantic(t_list);
							}
						}

						if(rc){
							printf("rc = %d\n", rc);
						}
						
					}
				}

				fflush(fhandle);
				fclose(fhandle);
			}
			else{
				printf("error opening db.log\n");
				rc = FILE_OPEN_ERROR;
			}


			//printf("fhandle offset is at %d\n", offset);
			//printf("existing_log\n%s",existing_log);
			//printf("redo log_log\n%s",redo);

			//rewrite log without RF_START
			if( (fhandle = fopen("db.log","w")) != NULL){
				fprintf(fhandle, existing_log);
				fprintf(fhandle, redo);

				fflush(fhandle);
				fclose(fhandle);
			}
			else{
				printf("error opening db.log\n");
				rc = FILE_OPEN_ERROR;
			}

		}
		else if ( (cur->tok_value == K_TO) && (cur->next->tok_value != EOC)){
			cur = cur->next;
			if(strlen(cur->tok_string) != 14){
				printf("can only rollforward to a given timestamp as yyyymmddhhmmss");
				rc = INVALID_TS_FOR_ROLLFORWARD;
				cur->tok_value = INVALID;
			}
			else{
				//printf("%s\n", cur->tok_string);
				//printf("cur tok value is %d and tok string is %s\n", cur->tok_value, cur->tok_string);
				now = get_timestamp();
				if(strcmp(now, cur->tok_string) < 0){
					printf("you are trying to rollforward to a future timestamp\n");
				}
				else{
					//printf("to redo:\n%s\n", redo);

					if( (fhandle = fopen("db.log","r")) != NULL){
						_fstat(_fileno(fhandle), &file_stat);
						//existing_log = (char*)calloc(1, file_stat.st_size);
						char ts[16];
						char *cmd = (char*)calloc(1, MAX_PRINT_LEN);
						char *bkup = (char*)calloc(1, MAX_PRINT_LEN);
						
						fseek(fhandle, offset, SEEK_SET);
						prune_redo = (char*)calloc(1,file_stat.st_size - offset);
						while(!feof(fhandle)){
							bool roll = false;
							if(fgets(ts,16,fhandle) != NULL){
								//strcat(redo,bkup);
								//printf("cur: '%s' and ts: '%s'\n", cur->tok_string, ts);
								if(strncmp(ts, cur->tok_string, 14) <= 0){
									roll = true;
									//printf("  rollfwd this one\n");
								}
							}

							//fseek(fhandle, 1, SEEK_CUR);//skip first quote
							if(fgets(bkup,MAX_PRINT_LEN,fhandle) != NULL){
								//strcat(redo,bkup);
								//printf("bkup is '%s' and length is %d\n",bkup,strlen(bkup)-2);
								//remove endline character and last quote
								if(roll){
									//add to pruned redo
									strcat(prune_redo,ts);
									strcat(prune_redo,bkup);

									char *cmd = (char*)calloc(1,strlen(bkup)-1); //need one extra for null terminator
									strncpy(cmd, bkup+1, strlen(bkup)-3);
									printf("rolling forward: '%s'\n", cmd);
									//printf("%d\n", strlen(cmd));

									token_list *t_list = NULL;
									rc = get_token(cmd, &t_list);
									//printf("rc is %d\n", rc);
									if(!rc){
										if(t_list->tok_value != K_BACKUP){//skip redoing of backup steps
											rc = do_semantic(t_list);
										}
									}

									if(rc){
										printf("rc = %d\n", rc);
									}
								}
							}
						}

						fflush(fhandle);
						fclose(fhandle);
					}
					else{
						printf("error opening db.log\n");
						rc = FILE_OPEN_ERROR;
					}

					//rewrite log file without RF_START
					if( (fhandle = fopen("db.log","w")) != NULL){
						fprintf(fhandle, existing_log);
						fprintf(fhandle, prune_redo);

						if(strlen(prune_redo) < strlen(redo)){
							FILE *flogger = NULL;
							printf("ts for rollforward caused pruning of the log.\n");

							char *temp = (char*)calloc(1, sizeof(int));
							int i = 1;
							itoa(i, temp, 10);
							char *log_name = (char*)calloc(1,6+sizeof(int));
							strcat(log_name, "db.log");
							strcat(log_name, temp);

							while((flogger = fopen(log_name, "r")) != NULL)
							{
								i++;
								//printf("i is now %d\n", i);
								memset(log_name, 0, 6+sizeof(int));
								memset(temp, 0, sizeof(int));
								strcat(log_name, "db.log");
								itoa(i, temp, 10);
								//printf("temp is %s\n", temp);
								strcat(log_name, temp);
								//printf("logname is now %s\n", log_name);
							}

							printf("original log file contents saved to %s\n", log_name);
							if((flogger = fopen(log_name, "w")) != NULL){
								fprintf(flogger, existing_log);
								fprintf(flogger, redo);
							}
							else{
								printf("error opening log file to prune\n");
								rc = FILE_OPEN_ERROR;
							}
						}

						fflush(fhandle);
						fclose(fhandle);
					}
					else{
						printf("error opening db.log\n");
						rc = FILE_OPEN_ERROR;
					}

					
				}//end else
			}
		}
		else{
			printf("keyword to in rollforward statement can only be followed by a 14 character timestamp\n");
			rc = INVALID_ROLLFOWARD_STMT;
			cur->tok_value = INVALID;
		}
	}
	
	return rc;
}

/*helper functions*/
int checkAggregateSyntax(tpd_entry *tab_entry, token_list *agg_col)
{	//checks token list syntax when includes aggregate function
	int rc = 0;
	token_list *agg_func = agg_col;
	//case 1: sum and avg must operate on a column of type int
	if( (agg_col->tok_value == F_SUM) || (agg_col->tok_value == F_AVG) )
	{
		if (agg_col->next->tok_value != S_LEFT_PAREN)
		{
			printf("invalid syntax for aggregate function. missing L parantheses\n");
			rc = INVALID_SYNTAX_FOR_AGGREGATE;
			agg_col->next->tok_value = INVALID;
		}
		else
		{
			agg_col = agg_col->next->next; //skip past L parentheses
			if (agg_col->tok_class != identifier)
			{
				printf("%s function must operate on a column\n", agg_func->tok_string);
				rc = INVALID_COL_FOR_AGGREGATE;
				agg_col->tok_value = INVALID;
			}
			else
			{	
				int column_number = columnFinder(tab_entry, agg_col->tok_string);
				if(column_number > -1){
					int type_match = checkColType(tab_entry, agg_col->tok_string, INT_LITERAL, column_number);
					if(type_match == -1)
					{ //not a type match
						printf("%s function must operate on a integer column\n", agg_func->tok_string);
							rc = AGGREGATE_COL_TYPE_MISMATCH;
							agg_col->tok_value = INVALID;
					}
					else{//check for R parantheses
						if(agg_col->next->tok_value != S_RIGHT_PAREN){
							printf("invalid syntax for aggregate function. missing R parantheses\n");
							rc = INVALID_SYNTAX_FOR_AGGREGATE;
							agg_col->next->tok_value = INVALID;
						}
						else{
							agg_col = agg_col->next->next;
							if(agg_col->tok_value != K_FROM){
								printf("syntax error: unexpected symbol or keyword in select statement\n");
								rc = INVALID_SELECT_STATEMENT;
								agg_col->tok_value = INVALID;
							}
							return column_number;
						}
					}
				}
				else{
					printf("column not found in table %s\n", tab_entry->table_name);
					rc = COLUMN_NOT_EXIST;
					agg_col->tok_value = INVALID;
				}
			}
		}
	}
	//case 2: count
	else if(agg_col->tok_value == F_COUNT){
		if (agg_col->next->tok_value != S_LEFT_PAREN)
		{
			printf("invalid syntax for aggregate function. missing L parantheses\n");
			rc = INVALID_SYNTAX_FOR_AGGREGATE;
			agg_col->next->tok_value = INVALID;
		}
		else
		{
			agg_col = agg_col->next->next; //skip past L parentheses
			if( (agg_col->tok_value == S_STAR) || (agg_col->tok_class == identifier) ){
				//can be count(*) or count(col_name)
				if(agg_col->next->tok_value != S_RIGHT_PAREN){
					printf("invalid syntax for aggregate function. missing R parantheses\n");
					rc = INVALID_SYNTAX_FOR_AGGREGATE;
					agg_col->next->tok_value = INVALID;
				}
				else{
					if(agg_col->next->next->tok_value != K_FROM){
						printf("syntax error: unexpected symbol or keyword in select statement\n");
						rc = INVALID_SELECT_STATEMENT;
						agg_col->tok_value = INVALID;
					}
					else{
						if(agg_col->tok_value == S_STAR){
							rc = 300; //for count(*) only
						}
						else if(agg_col->tok_class == identifier){
							int column_number = columnFinder(tab_entry, agg_col->tok_string);
							if(column_number > -1){
								return column_number;
							}
							else{
								printf("column not found in table %s\n", tab_entry->table_name);
								rc = COLUMN_NOT_EXIST;
								agg_col->tok_value = INVALID;
							}
						}
					}
				}
			}
			else{
				printf("syntax error: unexpected symbol or keyword in select statement\n");
				rc = INVALID_SELECT_STATEMENT;
				agg_col->tok_value = INVALID;
			}
		}
	}

	return rc;
}

int print_selectAll(tpd_entry *tab_entry)
{	//prints select * case
	struct _stat file_stat;
	table_file_header *recs = NULL;
	char str[MAX_IDENT_LEN];
	strcpy(str, tab_entry->table_name); //get table name
	strcat(str, ".tab");

	//get column headers
	char *format, *head;
	format = getOuter(tab_entry);
	head = getColHeaders(tab_entry);

	//print formatting
	printf("%s\n", format);
	printf("%s\n", head);
	printf("%s\n", format);

	FILE *fhandle = NULL;
	if ((fhandle = fopen(str, "rbc")) == NULL)
	{
		printf("there was an error opening the file %s\n",str);
		return -1;
	}//end file open error
	else
	{
		_fstat(_fileno(fhandle), &file_stat);
		recs = (table_file_header*)calloc(1, file_stat.st_size); //get all recs in the table.tab file
		if (!recs){
			printf("there was a memory error...\n");
			return -2;
		}//end memory error
		else
		{
			fread(recs, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			if (recs->file_size != file_stat.st_size){
				printf("ptr file_size: %d\n", recs->file_size);
				printf("read file size and calculated size does not match. db file is corrupted. exiting...\n");
				return -3;
			}
			else{
				int rows_tb_size = recs->file_size - recs->record_offset;
				int record_size = recs->record_size;
				int offset = recs->record_offset;
				int num_records = recs->num_records;

				fseek(fhandle, offset, SEEK_SET);
				unsigned char *buffer;
				buffer = (unsigned char*)calloc(1, record_size * 100);
				if (!buffer)
					return -2;
				else
				{
					fread(buffer, rows_tb_size, 1, fhandle);
					int i = 0, rec_cnt = 0, rows = 1, col_number = 0;
					while (i < rows_tb_size)
					{
						printf("|");
						cd_entry  *col = NULL;
						int k, row_col = 0;
						for (k = 0, col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
							k < tab_entry->num_columns; k++, col++)
						{	//while there are columns in tpd
							unsigned char col_len = (unsigned char)buffer[i];
							if ((int)col_len > 0) //item is not null
							{
								if (col->col_type == T_INT)
								{	//for integer data
									int b = i + 1;//move past length byte
									char *int_b;
									int elem;
									int_b = (char*)calloc(1, sizeof(int));
									for (int a = 0; a < sizeof(int); a++)
										int_b[a] = buffer[b + a]; //copy from buffer
									memcpy(&elem, int_b, sizeof(int));
									printf("%11d|", elem);
								}
								else if (col->col_type == T_CHAR)
								{	//for char data
									int b = i + 1;//move past length byte
									char *str_b;
									int len = col->col_len + 1;
									str_b = (char*)calloc(1, len);
									for (int a = 0; a < len; a++)
										str_b[a] = buffer[b + a];//copy from buffer
									str_b[len - 1] = '\0';
									printf("%s", str_b);
									int str_len = strlen(str_b);
									int width = (col->col_len > strlen(col->col_name)) ?
										col->col_len : strlen(col->col_name);
									while (str_len < width)
									{
										printf(" ");
										str_len++;
									}
									printf("|");
								}
							}
							else
							{	//for null
								if ((int)col_len == 0){
									int b = i + 1;
									char *null_b;
									int len = 0;
									if (col->col_type == T_INT)
										len = 11; //int max is 11 digits
									else if (col->col_type == T_CHAR)
									{
										len = (col->col_len > strlen(col->col_name)) ?
											col->col_len : strlen(col->col_name); //len is whichever is larger
									}

									null_b = (char*)calloc(1, len);
									strcat(null_b, "-");
									int str_len = strlen(null_b);
									int width = (str_len > len) ? str_len : len;

									//print align to L or R depending on col type
									if (col->col_type == T_INT)
									{
										while (str_len < width)
										{	//pad with spaces while data strlen less than calculated width
											printf(" ");
											str_len++;
										}
										printf("%s|", null_b);
									}
									else if (col->col_type == T_CHAR)
									{
										printf("%s", null_b);
										while (str_len < width)
										{	//pad with spaces while data strlen less than calculated width
											printf(" ");
											str_len++;
										}
										printf("|");
									}
								}//if column is null
								else
									return -3; //if byte is anything else
							}
							i += col->col_len+1; //move to next item/column
						}
						printf("\n");//print new line between records
						rec_cnt += record_size; //jump to next record
						i = rec_cnt; //move index variable i forward to next record
					}//end while
					if (num_records > 0)
						printf("%s\n", format); //print last formatting line
					printf("%d rows selected.\n", num_records);
				}//end not memory error
			}//file is not corrupt
			fclose(fhandle);
		}//end not memory error
		return 0;
	}//not file open error
}

int print_select(tpd_entry *tab_entry, int colArray[], int cols_to_print)
{	//prints select by column case
	struct _stat file_stat;
	table_file_header *recs = NULL;
	char str[MAX_IDENT_LEN];
	strcpy(str, tab_entry->table_name); //get table name
	strcat(str, ".tab");

	//get column headers
	char *format, *head;
	format = getOuterByCol(tab_entry, colArray, cols_to_print);
	head = getColHeadersByCol(tab_entry, colArray, cols_to_print);
	printf("%s\n", format); //print ascii table formatting
	printf("%s\n", head);	//print table col headings
	printf("%s\n", format);	//print ascii table formatting

	FILE *fhandle = NULL;
	if ((fhandle = fopen(str, "rbc")) == NULL)
	{
		printf("there was an error opening the file %s\n",str);
		return -1;
	}//end file open error
	else
	{
		_fstat(_fileno(fhandle), &file_stat);
		recs = (table_file_header*)calloc(1, file_stat.st_size);
		if (!recs)
		{
			printf("there was a memory error...\n");
			return -2;
		}//end memory error
		else
		{
			fread(recs, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			if (recs->file_size != file_stat.st_size)
			{
				printf("ptr file_size: %d\n", recs->file_size);
				printf("read file size and calculated size does not match. db file is corrupted. exiting...\n");
				return -3;
			}
			else
			{
				int rows_tb_size = recs->file_size - recs->record_offset;
				int record_size = recs->record_size;
				int offset = recs->record_offset;
				int num_records = recs->num_records;
				unsigned char *buffer;
				buffer = (unsigned char*)calloc(1, record_size * 100); //read max # records from the table.tab file
				if (!buffer)
					return -2;
				else
				{
					fseek(fhandle, offset, SEEK_SET);
					fread(buffer, rows_tb_size, 1, fhandle);
					int i = 0, rec_cnt = 0, rows = 1;
					while (i < rows_tb_size)
					{
						printf("|");
						cd_entry  *col = NULL;
						int k;

						for (int a = 0; a < cols_to_print; a++)
						{
							int row_col = i;
							for (k = 0, col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
								k < tab_entry->num_columns; k++, col++)
							{	//while there are columns in tpd
								unsigned char col_len = (unsigned char)buffer[row_col];
								if(colArray[a] == col->col_id)
								{
									if ((int)col_len > 0) ///column does not have null value
									{
										if (col->col_type == T_INT)
										{	//for integer data
											int b = row_col + 1;
											char *int_b;
											int elem;
											int_b = (char*)calloc(1, sizeof(int));
											for (int a = 0; a < sizeof(int); a++)
												int_b[a] = buffer[b + a];
											memcpy(&elem, int_b, sizeof(int));
											printf("%11d|", elem);
										}
										else if (col->col_type == T_CHAR)
										{	//for char data
											int b = row_col + 1;
											char *str_b;
											int len = col->col_len + 1;
											str_b = (char*)calloc(1, len);
											for (int a = 0; a < len; a++)
												str_b[a] = buffer[b + a];
											str_b[len - 1] = '\0';
											printf("%s", str_b);
											int str_len = strlen(str_b);
											int width = (col->col_len > strlen(col->col_name)) ?
												col->col_len : strlen(col->col_name);
											while (str_len < width)
											{
												printf(" ");
												str_len++;
											}
											printf("|");
										}
									}
									else
									{	//for null
										if ((int)col_len == 0)
										{
											int b = row_col + 1;
											char *null_b;
											int len = 0;
											if (col->col_type == T_INT)
												len = 11;
											else if (col->col_type == T_CHAR)
											{
												len = (col->col_len > strlen(col->col_name)) ?
													col->col_len : strlen(col->col_name);
											}

											null_b = (char*)calloc(1, len);
											strcat(null_b, "-");
											int str_len = strlen(null_b);
											int width = (str_len > len) ? str_len : len;

											//print align to L or R depending on col type
											if (col->col_type == T_INT)
											{
												while (str_len < width)
												{
													printf(" ");
													str_len++;
												}
												printf("%s|", null_b);

											}
											else if (col->col_type == T_CHAR)
											{
												printf("%s", null_b);
												while (str_len < width)
												{
													printf(" ");
													str_len++;
												}
												printf("|");
											}

										}//if column is null
										else
											return -3; //if byte is anything else
									}
								}
								row_col += col->col_len+1; //move to next item/column
							}
						}
						printf("\n");//print newline between records
						rec_cnt += record_size; //jump to next record
						i = rec_cnt;
					}//end while
					if (num_records > 0)
						printf("%s\n", format); //print last line
					printf("%d rows selected.\n", num_records);
				}//end not memory error
			}//file is not corrupt
			fclose(fhandle);
		}//end not memory error
		return 0;
	}//not file open error
}

int print_select_from_buffer(tpd_entry *tab_entry, unsigned char* buffer, int len_of_buffer, int num_records, int record_size)
{	//prints select * case with where or order by clauses
	//get column headers
	char *format, *head;
	format = getOuter(tab_entry);
	head = getColHeaders(tab_entry);
	printf("%s\n", format);
	printf("%s\n", head);
	printf("%s\n", format);

	int i = 0, rec_cnt = 0, rows = 1, col_number = 0;
	while (i < (num_records*record_size))
	{
		printf("|");
		cd_entry  *col = NULL;
		int k, row_col = 0;
		for (k = 0, col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			k < tab_entry->num_columns; k++, col++)
		{	//while there are columns in tpd
			unsigned char col_len = (unsigned char)buffer[i];
			if ((int)col_len > 0)
			{
				if (col->col_type == T_INT)
				{	//for integer data
					int b = i + 1;
					char *int_b;
					int elem;
					int_b = (char*)calloc(1, sizeof(int));
					for (int a = 0; a < sizeof(int); a++)
						int_b[a] = buffer[b + a];
					memcpy(&elem, int_b, sizeof(int));
					printf("%11d|", elem);
				}
				else if (col->col_type == T_CHAR)
				{	//for char data
					int b = i + 1;
					char *str_b;
					int len = col->col_len + 1;
					str_b = (char*)calloc(1, len);
					for (int a = 0; a < len; a++)
						str_b[a] = buffer[b + a];
					str_b[len - 1] = '\0';
					printf("%s", str_b);
					int str_len = strlen(str_b);
					int width = (col->col_len > strlen(col->col_name)) ?
						col->col_len : strlen(col->col_name);
					while (str_len < width)
					{
						printf(" ");
						str_len++;
					}
					printf("|");
				}
			}
			else
			{	//for null
				if ((int)col_len == 0)
				{
					int b = i + 1;
					char *null_b;
					int len = 0;
					if (col->col_type == T_INT)
						len = 11;
					else if (col->col_type == T_CHAR)
					{
						len = (col->col_len > strlen(col->col_name)) ?
							col->col_len : strlen(col->col_name);
					}

					null_b = (char*)calloc(1, len);
					strcat(null_b, "-");
					int str_len = strlen(null_b);
					int width = (str_len > len) ? str_len : len;

					//print align to L or R depending on col type
					if (col->col_type == T_INT)
					{
						while (str_len < width)
						{
							printf(" ");
							str_len++;
						}
						printf("%s|", null_b);
					}
					else if (col->col_type == T_CHAR)
					{
						printf("%s", null_b);
						while (str_len < width)
						{
							printf(" ");
							str_len++;
						}
						printf("|");
					}

				}//if column is null
				else
					return -3; //if byte is anything else
			}
			i += col->col_len+1; //move to next item/column
		}
		printf("\n"); //print newline between records
		rec_cnt += record_size; //jump to next record
		i = rec_cnt;
	}//end while
	if (num_records > 0)
		printf("%s\n", format); //print last line
	printf("%d rows selected.\n", num_records);
				
	return 0;
}

int print_select_from_buffer(tpd_entry *tab_entry, unsigned char* buffer, int len_of_buffer, int matches, int record_size,  int colArray[], int cols_to_print)
{	//overloaded func - prints select by col case with where or order by clauses
	//get column headers
	char *format, *head;
	format = getOuterByCol(tab_entry, colArray, cols_to_print);
	head = getColHeadersByCol(tab_entry, colArray, cols_to_print);
	printf("%s\n", format);
	printf("%s\n", head);
	printf("%s\n", format);

	int i = 0, rec_cnt = 0, rows = 1;
	while (i < len_of_buffer)
	{
		printf("|");
		cd_entry  *col = NULL;
		int k;
		for (int a = 0; a < cols_to_print; a++)
		{
			int row_col = i;
			for (k = 0, col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
				k < tab_entry->num_columns; k++, col++)
			{	//while there are columns in tpd
				unsigned char col_len = (unsigned char)buffer[row_col];
				if(colArray[a] == col->col_id)
				{
					if ((int)col_len > 0)
					{
						if (col->col_type == T_INT)
						{	//for integer data
							int b = row_col + 1;
							char *int_b;
							int elem;
							int_b = (char*)calloc(1, sizeof(int));
							for (int a = 0; a < sizeof(int); a++)
								int_b[a] = buffer[b + a];
							memcpy(&elem, int_b, sizeof(int));
							printf("%11d|", elem);
						}
						else if (col->col_type == T_CHAR)
						{	//for char data
							int b = row_col + 1;
							char *str_b;
							int len = col->col_len + 1;
							str_b = (char*)calloc(1, len);
							for (int a = 0; a < len; a++)
								str_b[a] = buffer[b + a];
							str_b[len - 1] = '\0';
							printf("%s", str_b);
							int str_len = strlen(str_b);
							int width = (col->col_len > strlen(col->col_name)) ?
								col->col_len : strlen(col->col_name);
							while (str_len < width)
							{
								printf(" ");
								str_len++;
							}
							printf("|");
						}
					}
					else
					{	//for null
						if ((int)col_len == 0)
						{
							int b = row_col + 1;
							char *null_b;
							int len = 0;
							if (col->col_type == T_INT)
								len = 11;
							else if (col->col_type == T_CHAR)
							{
								len = (col->col_len > strlen(col->col_name)) ?
									col->col_len : strlen(col->col_name);
							}

							null_b = (char*)calloc(1, len);
							strcat(null_b, "-");
							int str_len = strlen(null_b);
							int width = (str_len > len) ? str_len : len;

							//print align to L or R depending on col type
							if (col->col_type == T_INT)
							{
								while (str_len < width)
								{
									printf(" ");
									str_len++;
								}
								printf("%s|", null_b);
							}
							else if (col->col_type == T_CHAR)
							{
								printf("%s", null_b);
								while (str_len < width)
								{
									printf(" ");
									str_len++;
								}
								printf("|");
							}
						}//if column is null
					}
				}
				row_col += col->col_len+1; //move to next item/column
			}
		}
		printf("\n");//print new line btwn records
		rec_cnt += record_size; //jump to next record
		i = rec_cnt;
	}//end while

	if (matches > 0)
		printf("%s\n", format); //print last line
	printf("%d rows selected.\n", matches);
				
	return 0;
}

int print_aggregate(tpd_entry *tab_entry, table_file_header *table_info, unsigned char* buffer, int column_number, token_list *agg_tok, int agg_func, int num_records)
{	//print select by aggregate func case
	int rc = 0;
	char col_name[MAX_IDENT_LEN];
	strcpy(col_name, agg_tok->tok_string);
	
	//figure out width of header
	int str_len = strlen(col_name);
	int width = 12;
	if(str_len > 7){
		width = str_len + 5;
	}
	
	char *format = (char*)calloc(1, MAX_PRINT_LEN);
	strcpy(format,"+");
	for (int z = 0; z < width; z++){
		strcat(format,"-");
	}
	strcat(format,"+");

	int diff = width - (str_len + 5); //for header
	int d = width - 12;	//for value
	int row_cnt = cnt_not_null(tab_entry, table_info, buffer, column_number, num_records);
	int sum = select_helper_math(tab_entry, table_info, buffer, column_number, num_records);
	switch(agg_func){
		case 41: //sum
			if(sum < 0){
				switch(sum)
				{
					case -1: 
						rc = FILE_OPEN_ERROR;
						break;
					case -2:
						rc = MEMORY_ERROR;
						break;
					case -3:
						rc = DBFILE_CORRUPTION;
						break;
				}
			}
			else{
				//print sum
				printf("%s\n",format);
				printf("|sum(%s)",col_name);
				for (int y = 0; y < diff; y++){
					printf(" ");
				}
				printf("|\n");
				printf("%s\n",format);
				printf("|");
				for (int x = 0; x < d; x++)
					printf(" ");
				printf("%12d|\n", sum);
				printf("%s\n",format);
			}
			break;
		case 42: //avg
			printf("%s\n",format);
			if(row_cnt < 0){
				switch(row_cnt)
				{
					case -1: 
						rc = FILE_OPEN_ERROR;
						break;
					case -2:
						rc = MEMORY_ERROR;
						break;
					case -3:
						rc = DBFILE_CORRUPTION;
						break;
				}
				return rc;
			}
			else if (row_cnt == 0){
				printf("|avg(%s)",col_name);
				for (int y = 0; y < diff; y++){
					printf(" ");
				}
				printf("|\n");
				printf("%s\n",format);
				printf("|");
				for (int x = 0; x < d; x++)
					printf(" ");
				printf("%12d|\n", 0);
			}
			else{
				float avg = (float) sum / row_cnt;
				printf("|avg(%s)",col_name);
				for (int y = 0; y < diff; y++){
					printf(" ");
				}
				printf("|\n");
				printf("%s\n",format);
				printf("|");
				for (int x = 0; x < d; x++)
					printf(" ");
				printf("%12.1f|\n", avg);
			}
			printf("%s\n",format);
			break;
		case 43: //count
			if(column_number == 300){
				printf("+--------+\n");
				printf("|count(*)|\n");
				printf("+--------+\n");
				printf("|%8d|\n", num_records);
				printf("+--------+\n");
			}
			else{
				//figure out width of header
				int str_len = strlen(col_name);
				int width = 12;
				if(str_len > 5){
					width = str_len + 7;
				}
				
				char *format = (char*)calloc(1, MAX_PRINT_LEN);
				strcpy(format,"+");
				for (int z = 0; z < width; z++){
					strcat(format,"-");
				}
				strcat(format,"+");

				int diff = width - (str_len + 7); //for header
				int d = width - 12;	//for value
				
				//print first format line
				printf("%s\n",format);
				printf("|count(%s)",col_name);
				for (int y = 0; y < diff; y++){
					printf(" ");
				}
				printf("|\n");
				printf("%s\n",format);
				printf("|");
				for (int x = 0; x < d; x++)
					printf(" ");
				printf("%12d|\n", row_cnt);
				printf("%s\n",format);
			}
			break;
	}//end switch
	printf("1 rows selected.\n");
	return rc;
}

int select_helper(tpd_entry *tab_entry)
{	//returns number of records of a table
	char str[MAX_IDENT_LEN];
	struct _stat file_stat;
	table_file_header *head = NULL;
	strcpy(str, tab_entry->table_name);
	strcat(str, ".tab");

	FILE *fhandle = NULL;
	if ((fhandle = fopen(str, "rbc")) == NULL)
	{
		printf("there was an error opening the file %s\n",str);
		return -1;
	}//end file open error
	else
	{
		_fstat(_fileno(fhandle), &file_stat);
		head = (table_file_header*)calloc(1, file_stat.st_size);
		if (!head)
		{
			printf("there was a memory error...\n");
			return -2;
		}//end memory error
		else
		{
			fread(head, file_stat.st_size, 1, fhandle);
			if (head->file_size != file_stat.st_size)
			{
				printf("read file size and calculated size does not match. db file is corrupted. exiting...\n");
				return -3;
			}
			else
			{
				int num_records = head->num_records;
				return num_records;
			}
		}
	}
}

int select_helper_math(tpd_entry *tab_entry, table_file_header *table_info, unsigned char* buffer, int col_to_aggregate, int num_records)
{	//sums the column values in the buffer
	//int num_records = table_info->num_records;
	int record_size = table_info->record_size;
	int rows_tb_size = record_size*num_records;
	int offset = table_info->record_offset;
	int i = 0, rec_cnt = 0, rows = 1;
	int sum_to_return = 0;
	while (i < rows_tb_size)
	{
		cd_entry  *col = NULL;
		int k;
		int row_col = i;
		for (k = 0, col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			k < tab_entry->num_columns; k++, col++)
		{	//while there are columns in tpd
			unsigned char col_len = (unsigned char)buffer[row_col];
			if(col_to_aggregate == col->col_id)
			{
				if ((int)col_len > 0)
				{
					int b = row_col + 1;
					char *int_b;
					int elem;
					int_b = (char*)calloc(1, sizeof(int));
					for (int a = 0; a < sizeof(int); a++)
					{
						int_b[a] = buffer[b + a];
					}
					memcpy(&elem, int_b, sizeof(int));
					sum_to_return += elem;
				}
				//skip nulls (don't add)
			}
			row_col += col->col_len+1; //move to next item/column
		}
		rec_cnt += record_size; //jump to next record
		i = rec_cnt;
	}//end while
	
	return sum_to_return;	
}

int cnt_not_null(tpd_entry *tab_entry, table_file_header *table_info, unsigned char* buffer, int col_to_cnt, int num_records)
{	//count the number of not null values in the buffer specified by col#
	int record_size = table_info->record_size;
	int offset = table_info->record_offset;
	int rows_tb_size = record_size*num_records;
	int i = 0, rec_cnt = 0, rows = 1;
	int cnt_to_return = 0;
	while (i < rows_tb_size)
	{
		cd_entry  *col = NULL;
		int k;
		int row_col = i;
		for (k = 0, col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			k < tab_entry->num_columns; k++, col++)
		{	//while there are columns in tpd
			unsigned char col_len = (unsigned char)buffer[row_col];
			if(col_to_cnt == col->col_id)
			{
				if ((int)col_len > 0)
					cnt_to_return++;
			}
			row_col += col->col_len+1; //move to next item/column
		}
		rec_cnt += record_size; //jump to next record
		i = rec_cnt;
	}//end while
	return cnt_to_return;	
}

token_list * insertHelper(int tok_class, int tok_value, char* tok_string)
{	//creates a token to help with insert (added to a token list)
	token_list *temp = (token_list *)calloc(1, sizeof(token_list));
	temp->tok_class = tok_class;
	temp->tok_value = tok_value;	
	strcpy(temp->tok_string, tok_string);
	temp->next = NULL;
	return temp;
}//insertHelper

char* getOuter(tpd_entry *tab_entry)
{	//return string of the format of the ascii table
	char *format = (char*)calloc(1, MAX_PRINT_LEN);
	strcpy(format,"+");
	cd_entry  *col_entry = NULL;
	int i;
	for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
		i < tab_entry->num_columns; i++, col_entry++)
	{	//while there are columns in tpd
		//test if col_len is longer or col_name is longer - use whichever is longer
		int width = (col_entry->col_len > strlen(col_entry->col_name)) ? 
							col_entry->col_len : strlen(col_entry->col_name);
		if (col_entry->col_type == T_INT)
			width = 11; //int columns should have width 11
		for (int j = 0; j < width; j++)
			strcat(format, "-");
		strcat(format, "+");
	}
	return format;
}//getOuter

char* getColHeaders(tpd_entry *tab_entry)
{	//return string of the column headers of table
	char *head = (char*)calloc(1, MAX_PRINT_LEN);
	strcpy(head, "|");
	cd_entry  *col_entry = NULL;
	int i;
	for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
		i < tab_entry->num_columns; i++, col_entry++)
	{	//while there are columns in tpd	
		//test if col_len is longer or col_name is longer - use whichever is longer
		int width = (col_entry->col_len > strlen(col_entry->col_name)) ? 
							col_entry->col_len : strlen(col_entry->col_name);
		if (col_entry->col_type == T_INT)
			width = 11; //int columns should have width 11
		int diff = width - strlen(col_entry->col_name);
		strcat(head, col_entry->col_name);
		if (diff > 0)
		{
			for (int k = 0; k < diff; k++)
				strcat(head, " ");
		}
		strcat(head, "|");
	}
	return head;
}//getColHeaders()

int columnFinder(tpd_entry *tab_entry, char *tok_string)
{	//return column number given a tok_string of col_name and table
	cd_entry  *col_entry = NULL;
	int i, col_id = 0, col_type = 0, col_offset = 0, col_length = 0;
	for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++)
	{
		if(strcmp(col_entry->col_name, tok_string) == 0)
			return col_entry->col_id;//return col_id
		else //move to next column
			col_offset += col_entry->col_len + 1; //plus one for len byte
	}//end for
	return -1;//if not found, return -1
}//columnFinder()

int checkColType(tpd_entry *tab_entry, char *tok_string, int t_type, int c_num)
{	//check the token given matches the col_type specified
	cd_entry *col_entry = NULL;
	int j;
	for (j = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); j <= c_num; j++, col_entry++)
	{
		if(j == c_num)
		{
			if( ((col_entry->col_type == T_INT) && (t_type == INT_LITERAL)) ||
				((col_entry->col_type == T_CHAR) && (t_type == STRING_LITERAL)) )
			{
				if(t_type == STRING_LITERAL)
				{	//check for string length
					return checkCharLen(tab_entry, tok_string, c_num);
				}
				else if(t_type == INT_LITERAL)
				{	//check for size of integer
					return checkIntSize(tok_string);
				}
			}
			else if (t_type == K_NULL)
			{
				if(col_entry->not_null)
				{
					return -2;
				}
				else
					return 1;
			}
		}
	}
	return -1; //-1 for type mismatch
}//checkColType()

int checkCharLen(tpd_entry *tab_entry, char *tok_string, int c_num)
{	//check that the char token given does not exceed length of col definition
	cd_entry *col_entry = NULL;
	int j;
	for (j = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); j <= c_num; j++, col_entry++)
	{
		if(j == c_num)
		{
			if(strlen(tok_string) <= col_entry->col_len)
				return 1;
			else
				return 0;
		}
	}//end for
	return 0;
}//checkCharLen()

int checkIntSize(char *tok_string)
{	//check that token given as int does not exceed max int size supported
	if (strlen(tok_string) > 10)
		return 0;
	else if ((strlen(tok_string) == 10) && (strcmp(tok_string, "2147483647") > 0))
		return 0;
	else
		return 1;
}//checkIntSize()

int checkRowsForValue(tpd_entry *tab_entry, int col_to_update, token_list *update_token, int rel_op, token_list *where_token, int c_num)
{	//check rows for matching values for update
	char str[MAX_IDENT_LEN];
	struct _stat file_stat;
	table_file_header *head = NULL;
	unsigned char *buffer, *temp;
	strcpy(str, tab_entry->table_name);
	strcat(str, ".tab");
	FILE *fhandle = NULL;
	if ((fhandle = fopen(str, "rbc")) == NULL)
	{
		printf("there was an error opening the file %s\n",str);
		return -1;
	}//end file open error
	else
	{
		_fstat(_fileno(fhandle), &file_stat);
		head = (table_file_header*)calloc(1, file_stat.st_size);
		if (!head)
		{
			printf("there was a memory error...\n");
			return -2;
		}//end memory error
		else
		{
			fread(head, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			if (head->file_size != file_stat.st_size){
				printf("read file size and calculated size does not match. db file is corrupted. exiting...\n");
				return -3;
			}
			else{
				int rows_tb_size = head->file_size - head->record_offset;
				int record_size = head->record_size;
				int offset = head->record_offset;
				int num_records = head->num_records;
				fseek(fhandle, offset, SEEK_SET);
				buffer = (unsigned char*)calloc(1, record_size * num_records);
				if(!buffer)
					return -2;
				else{
					fread(buffer, record_size * num_records, 1, fhandle);
					fflush(fhandle);
					cd_entry *col = NULL;
					int i = 0, j = 0, rec_cnt = 0, row = 1, matches = 0;
					while(i < rows_tb_size){
						j = i;
						bool update = false;
						int k, col_offset = 0;
						for (k = 0, col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
							k < tab_entry->num_columns; k++, col++)
						{	//while there are columns in tpd
							if(col->col_id != c_num){
								i += col->col_len + 1;
								col_offset += col->col_len + 1;
							}
							else{
								int nullable = buffer[i];
								int b = i + 1;
								if(where_token->tok_value == K_NULL){
									int zero = 0;
									if(memcmp(&buffer[i], &zero, 1) == 0){
										matches++;
										update = true;
									}
								}
								else{
									if(col->col_type == T_INT){
										int elem;
										if(!nullable){
											elem = -99; //for null
										}//elem is null
										else{
											char *int_b;
											int_b = (char*)calloc(1, sizeof(int));
											for (int a = 0; a < sizeof(int); a++)
												int_b[a] = buffer[b + a];
											memcpy(&elem, int_b, sizeof(int));
											//check for matching rows depending on relational operator
											switch(rel_op)
											{
												case S_EQUAL:
													if(atoi(where_token->tok_string) == elem){
														matches++;
														update = true;
													}
													break;
												case S_LESS:
													if(elem < atoi(where_token->tok_string)){
														matches++;
														update = true;
													}
													break;
												case S_GREATER:
													if(elem > atoi(where_token->tok_string)){
														matches++;
														update = true;
													}
													break;
											}
										}//elem is not null
									}
									else if (col->col_type == T_CHAR){
										char *elem;
										if(!nullable){
											elem = NULL;
										}//elem is null
										else{
											int len = col->col_len + 1;
											elem = (char*)calloc(1, len);
											for (int a = 0; a < len; a++)
												elem[a] = buffer[b + a];
											elem[len - 1] = '\0';
											//check for value based on relational operator
											switch(rel_op)
											{
												case S_EQUAL:
													if (memcmp(where_token->tok_string, elem, col->col_len) == 0){
														matches++;
														update = true;
													}
													break;
												case S_LESS:
													if (memcmp(where_token->tok_string, elem, col->col_len) < 0){
														matches++;
														update = true;
													}
													break;
												case S_GREATER:
													if (memcmp(where_token->tok_string, elem, col->col_len) > 0){
														matches++;
														update = true;
													}
													break;
											}										
										}
									}
								}//not testing for null in where clause
							}//column id matches
						}//end for

						if (update)
						{
							for (k = 0, col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
							k < tab_entry->num_columns; k++, col++)
							{	//while there are columns in tpd
								if(col->col_id != col_to_update){
									j += col->col_len + 1;
									col_offset += col->col_len + 1;
								}
								else{
									if(update_token->tok_value == K_NULL)
									{	//printf("special set to null here\n");
										int sizer = 0;
										memcpy(&buffer[j], &sizer, sizeof(unsigned char));
										memcpy(&buffer[j+1], &sizer, col->col_len);
									}//null update
									else
									{
										int nullable = buffer[i];
										int b = j + 1;
										if(col->col_type == T_INT){
											int new_value = atoi(update_token->tok_string);
											int elem;
											if(!nullable){
												int int_size = sizeof(int);
												memcpy(&buffer[j], &int_size, sizeof(unsigned char));
											}//elem is nul l
											memcpy(&buffer[j+1], &new_value, sizeof(int)); //set size of int

											char *int_b;
											int_b = (char*)calloc(1, sizeof(int));
											for (int a = 0; a < sizeof(int); a++)
												int_b[a] = buffer[b + a];
											memcpy(&elem, int_b, sizeof(int)); //copy int element
										}
										else if (col->col_type == T_CHAR)
										{
											int new_strlen = strlen(update_token->tok_string);
											char *elem;
											int len = col->col_len;
											if(!nullable){
												elem = NULL;
											}//elem is null
											memcpy(&buffer[j], &new_strlen, sizeof(unsigned char));
											memcpy(&buffer[j+1], update_token->tok_string, len);
										}
									}
								}
							}//end for
						}
						row++;
						rec_cnt += record_size; //jump to next record
						i = rec_cnt;
					}//end while

					if ((fhandle = fopen(str, "wbc")) == NULL){
						return -1;
					}//end file open error
					else{
						fwrite(head, sizeof(table_file_header), 1, fhandle);
						fwrite(buffer, rows_tb_size, 1, fhandle);
						fflush(fhandle);
						fclose(fhandle);
						printf("%d rows updated.\n", matches);
					}
					return matches;
				}//end buffer else
			}//end file size ok
		}//end not memory error
	}//end not file open error
}//checkRowsForValue()

int updateHelper(tpd_entry *tab_entry, int col_to_update, token_list *update_token)
{	//update helper that updates col values with update_token 
	char str[MAX_IDENT_LEN];
	table_file_header *head = NULL;
	unsigned char *buffer;
	struct _stat file_stat;
	strcpy(str, tab_entry->table_name);
	strcat(str, ".tab");
	
	FILE *fhandle = NULL;
	//Open file for read
	if ((fhandle = fopen(str, "rbc")) == NULL){
		return -1;
	}//end file open error
	else{
		_fstat(_fileno(fhandle), &file_stat);
		head = (table_file_header*)calloc(1, file_stat.st_size);
		if(!head){
			return -2;
		}//memory error
		else{
			fread(head, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			if (head->file_size != file_stat.st_size){
				return -3; //dbfile corruption
			}
			else{
				int num_records = head->num_records;
				int offset = head->record_offset;
				int record_size = head->record_size;
				int rows_tb_size = head->file_size - head->record_offset;
				fseek(fhandle, offset, SEEK_SET);
				buffer = (unsigned char*)calloc(1, record_size * num_records);
				if(!buffer){
					return -2;
				}
				else{
					fread(buffer, rows_tb_size, 1, fhandle);
					cd_entry *col = NULL;
					int i = 0, rec_cnt = 0, row = 1, matches = 0;
					while(i < rows_tb_size)
					{
						int k, col_offset = 0;
						for (k = 0, col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
							k < tab_entry->num_columns; k++, col++)
						{	//while there are columns in tpd
							if(col->col_id != col_to_update){
								i += col->col_len + 1;
								col_offset += col->col_len + 1;
							}//continue until read specified column
							else{
								if(update_token->tok_value == K_NULL){//set to null
									int sizer = 0;
									memcpy(&buffer[i], &sizer, sizeof(unsigned char));
									memcpy(&buffer[i+1], &sizer, col->col_len);
								}//null update
								else{
									int nullable = buffer[i];
									int b = i + 1;
									if(col->col_type == T_INT){
										int new_value = atoi(update_token->tok_string);
										int elem;
										if(!nullable){
											int int_size = sizeof(int);
											memcpy(&buffer[i], &int_size, sizeof(unsigned char));
											//elem = -99; //temp hack for null
										}//elem is nul l
										memcpy(&buffer[i+1], &new_value, sizeof(int));
									}
									else if (col->col_type == T_CHAR){
										int new_strlen = strlen(update_token->tok_string);
										char *elem;
										int len = col->col_len;
										if(!nullable)
										{
											elem = NULL;
										}//elem is null
										memcpy(&buffer[i], &new_strlen, sizeof(unsigned char));
										memcpy(&buffer[i+1], update_token->tok_string, len);
									}
								}//regular update
							}
						}//end for
						row++;
						rec_cnt += record_size; //jump to next record
						i = rec_cnt;
					}//end while
					fclose(fhandle);
					if ((fhandle = fopen(str, "wbc")) == NULL){
						return -1;
					}//end file open error
					else{
						fwrite(head, sizeof(table_file_header), 1, fhandle);
						fwrite(buffer, rows_tb_size, 1, fhandle);
						fflush(fhandle);
						fclose(fhandle);
						printf("%d rows updated.\n", num_records);
					}
				}
			}
			return 1;
		}
	}
}//updateHelper()

int deleteHelper(char *table_name)
{	//delete helper calculates number of rows deleted
	char str[MAX_IDENT_LEN + 1];
	struct _stat file_stat;
	table_file_header *header = NULL;
	int rows_to_delete = 0;

	strcpy(str,table_name); //get table name
	strcat(str, ".tab");
	FILE *fhandle = NULL;

	if ((fhandle = fopen(str, "rbc")) == NULL)
		return -2;
	else
	{
		_fstat(_fileno(fhandle), &file_stat);
		header = (table_file_header*)calloc(1, file_stat.st_size);
		if(!header)
			return -1;
		else{
			fread(header, sizeof(table_file_header), 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);
			rows_to_delete = header->num_records;
			header->num_records = 0; // update to zero records
			header->file_size = sizeof(table_file_header);
			if ((fhandle = fopen(str, "wbc")) == NULL)
				return -2;
			else{
				fwrite(header, sizeof(table_file_header), 1, fhandle);
				fflush(fhandle);
				fclose(fhandle);
				printf("%d rows deleted.\n", rows_to_delete);
			}
		}//not memory error	
	}//file open OK
	return rows_to_delete;
}//deleteHelper()

int checkRowsForDelete(tpd_entry *tab_entry, int rel_op, token_list *where_token, int c_num)
{	//check which rows to delete
	char str[MAX_IDENT_LEN];
	struct _stat file_stat;
	table_file_header *head = NULL;
	unsigned char *buffer, *temp;
	strcpy(str, tab_entry->table_name);
	strcat(str, ".tab");
	FILE *fhandle = NULL;
	if ((fhandle = fopen(str, "rbc")) == NULL)
	{
		printf("there was an error opening the file %s\n",str);
		return -1;
	}//end file open error
	else
	{
		_fstat(_fileno(fhandle), &file_stat);
		head = (table_file_header*)calloc(1, file_stat.st_size);
		if (!head)
		{
			printf("there was a memory error...\n");
			return -2;
		}//end memory error
		else
		{
			fread(head, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			if (head->file_size != file_stat.st_size)
			{
				printf("read file size and calculated size does not match. db file is corrupted. exiting...\n");
				return -3;
			}
			else{
				int rows_tb_size = head->file_size - head->record_offset;
				int record_size = head->record_size;
				int offset = head->record_offset;
				int num_records = head->num_records;
				fseek(fhandle, offset, SEEK_SET); //skip to offset for records
				buffer = (unsigned char*)calloc(1, record_size * num_records);
				if(!buffer)
					return -2;
				else
				{
					fread(buffer, record_size * num_records, 1, fhandle);
					fflush(fhandle);
					cd_entry *col = NULL;
					int i = 0, j = 0, rec_cnt = 0, row = 1, matches = 0;
					int last_record = rows_tb_size - record_size;
					temp = (unsigned char*)calloc(1, record_size);
					while(i < rows_tb_size)
					{
						j = i;
						bool to_delete = false;
						int k, col_offset = 0;
						for (k = 0, col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
							k < tab_entry->num_columns; k++, col++)
						{	//while there are columns in tpd
							if(col->col_id != c_num){
								i += col->col_len + 1;
								col_offset += col->col_len + 1;
							}
							else{
								int nullable = buffer[i];
								int b = i + 1;
								if(where_token->tok_value == K_NULL){
									int zero = 0;
									if(memcmp(&buffer[i], &zero, 1) == 0){
										matches++;
										to_delete = true;
									}
									if(memcmp(&buffer[last_record], &zero, 1) == 0){
										last_record -= record_size;
									}
								}
								else{
									if(col->col_type == T_INT){//for int columns
										int elem;
										int last_elem;
										if(!nullable){
											elem = -99; //for null
										}//elem is null
										else
										{
											char *int_b, *int_b2;
											int_b = (char*)calloc(1, sizeof(int));
											for (int a = 0; a < sizeof(int); a++)
												int_b[a] = buffer[b + a];
											memcpy(&elem, int_b, sizeof(int));
											int_b2 = (char*)calloc(1, sizeof(int));
											//check last element (which will replace the cur element)
											for (int a = 0; a < sizeof(int); a++)
												int_b2[a] = buffer[last_record+col_offset+1 + a];
											memcpy(&last_elem, int_b2, sizeof(int));

											switch(rel_op)
											{
												case S_EQUAL:
													if(atoi(where_token->tok_string) == elem){
														matches++;
														to_delete = true;
													}
													if(atoi(where_token->tok_string) == last_elem){
														last_record -= record_size;//skip this last elem bc it equals the delete condition as well
													}
													break;
												case S_LESS:
													if(elem < atoi(where_token->tok_string)){
														matches++;
														to_delete = true;
													}
													if(last_elem < atoi(where_token->tok_string)){
														last_record -= record_size;//skip last elemn bc it equals delete condition as well
													}
													break;
												case S_GREATER:
													if(elem > atoi(where_token->tok_string)){
														matches++;
														to_delete = true;
													}
													if(last_elem > atoi(where_token->tok_string)){
														last_record -= record_size;//skip last elem bc it equals delete condition as well
													}
													break;
											}
										}//elem is not null
									}
									else if (col->col_type == T_CHAR){//char columns
										char *elem, *last_elem;
										if(!nullable)
										{
											elem = NULL;
										}//elem is null
										else
										{
											int len = col->col_len + 1;
											elem = (char*)calloc(1, len);
											for (int a = 0; a < len; a++)
												elem[a] = buffer[b + a];
											elem[len - 1] = '\0';

											//check last element too (will be used to replace cur element)
											last_elem = (char*)calloc(1, len);
											for (int a = 0; a < len; a++)
											{
												last_elem[a] = buffer[last_record+col_offset+1 + a];
											}
											last_elem[len - 1] = '\0';
											
											switch(rel_op)
											{
												case S_EQUAL:
													if (memcmp(where_token->tok_string, elem, col->col_len) == 0){
														matches++;
														to_delete = true;
													}
													if (memcmp(where_token->tok_string, last_elem, col->col_len) == 0){ //skip last record bc it matches delete cond too
														last_record -= record_size;
													}
													break;
												case S_LESS:
													if (memcmp(where_token->tok_string, elem, col->col_len) < 0){
														matches++;
														to_delete = true;
													}
													if (memcmp(where_token->tok_string, last_elem, col->col_len) < 0){//skip last rec bc it matches delete cond
														last_record -= record_size;
													}
													break;
												case S_GREATER:
													if (memcmp(where_token->tok_string, elem, col->col_len) > 0){
														matches++;
														to_delete = true;
													}
													if (memcmp(where_token->tok_string, last_elem, col->col_len) > 0){ //skip last rec bc it matches delete cond
														last_record -= record_size;
													}
													break;
											}										
										}
									}
								}//not testing for null in where clause
								break;
							}//column id matches
						}//end for

						if(to_delete)
						{
							for (int a = 0; a < record_size; a++)
							{
								temp[a] = buffer[last_record + a];
							}
						
							for (int a = 0; a < record_size; a++)
							{//do the replacement
								buffer[j+a] = temp[a];
							}
							head->num_records--; //each delete, decrement num_records
							head->file_size -= record_size;	//decrease file size by 1 record
							last_record -= record_size; //move last_record to the last previous record before it
						}
						row++;
						rec_cnt += record_size; //jump to next record
						i = rec_cnt;
					}//end while

					if(matches > 0)
					{//write the new buffer to the table file
						if ((fhandle = fopen(str, "wbc")) == NULL)
						{
							return -1;
						}//end file open error
						else
						{
							fwrite(head, sizeof(table_file_header), 1, fhandle);
							fwrite(buffer, record_size * head->num_records, 1, fhandle);
							fflush(fhandle);
							fclose(fhandle);
							printf("%d rows deleted.\n", matches);
						}
					}
					return matches;	//returns number of rows deleted
				}//end buffer else
			}//end file size ok
		}//end not memory error
	}//end not file open error
}//checkRowsForDelete()

char* getOuterByCol(tpd_entry *tab_entry, int colArray[], int num_col_to_fetch)
{	//get ascii table for when columns specified
	char *format = (char*)calloc(1, MAX_PRINT_LEN);
	strcpy(format,"+");
	cd_entry  *col_entry = NULL;
	int i;

	for(int j = 0; j < num_col_to_fetch; j++)
	{
		for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			i < tab_entry->num_columns; i++, col_entry++)
		{	//while there are columns in tpd
			if(col_entry->col_id == colArray[j]){
				//test if col_len is longer or col_name is longer - use whichever is longer
				int width = (col_entry->col_len > strlen(col_entry->col_name)) ? 
									col_entry->col_len : strlen(col_entry->col_name);
				if (col_entry->col_type == T_INT)
					width = 11; //int columns should have width 11
				for (int j = 0; j < width; j++)
					strcat(format, "-");
				strcat(format, "+");
			}
		}
	}
	return format;
}//getOuterByCol

char* getColHeadersByCol(tpd_entry *tab_entry, int colArray[], int num_col_to_fetch)
{	//get col headers when columns specified
	char *head = (char*)calloc(1, MAX_PRINT_LEN);
	strcpy(head, "|");
	cd_entry  *col_entry = NULL;
	int i;
	for (int j = 0; j < num_col_to_fetch; j++){
		for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
			i < tab_entry->num_columns; i++, col_entry++)
		{	//while there are columns in tpd
			if(col_entry->col_id == colArray[j]){
				//test if col_len is longer or col_name is longer - use whichever is longer
				int width = (col_entry->col_len > strlen(col_entry->col_name)) ? 
									col_entry->col_len : strlen(col_entry->col_name);
				if (col_entry->col_type == T_INT)
					width = 11; //int columns should have width 11

				int diff = width - strlen(col_entry->col_name);
				strcat(head, col_entry->col_name);
				if (diff > 0)
				{
					for (int k = 0; k < diff; k++)
						strcat(head, " ");
				}
				strcat(head, "|");
			}
		}
	}
	return head;
}//getColHeadersByCol()

unsigned char* get_buffer(tpd_entry *tab_entry)
{	//get the full contents of a table and return as a string buffer
	struct _stat file_stat;
	table_file_header *recs = NULL;
	char str[MAX_IDENT_LEN];
	strcpy(str, tab_entry->table_name); //get table name
	strcat(str, ".tab");

	FILE *fhandle = NULL;
	if ((fhandle = fopen(str, "rbc")) == NULL){
		printf("there was an error opening the file %s\n",str);
		//return -1;
		return NULL;
	}//end file open error
	else
	{
		_fstat(_fileno(fhandle), &file_stat);
		recs = (table_file_header*)calloc(1, file_stat.st_size);
		if (!recs){
			printf("there was a memory error...\n");
			return NULL;
		}//end memory error
		else
		{
			fread(recs, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			if (recs->file_size != file_stat.st_size){
				printf("ptr file_size: %d\n", recs->file_size);
				printf("read file size and calculated size does not match. db file is corrupted. exiting...\n");
				return NULL;
			}
			else{
				int rows_tb_size = recs->file_size - recs->record_offset;
				int record_size = recs->record_size;
				int offset = recs->record_offset;
				int num_records = recs->num_records;
				fseek(fhandle, offset, SEEK_SET);//skip to offset of records
				unsigned char *buffer;
				buffer = (unsigned char*)calloc(1, record_size * 100);
				if (!buffer)
					return NULL;//return -2;
				else
				{
					fread(buffer, rows_tb_size, 1, fhandle);
					return buffer;//return the buffer
				}
			}
		}
	}
}

unsigned char* selectRowsForValue(unsigned char* buffer, tpd_entry *tab_entry, int col_to_search, token_list *search_token, int rel_op, int rec_cnt, int rec_size)
{	//return a string buffer of rows that match the where condition in select statmenet
	/*possible values of rel_op: 
	S_EQUAL - 74, S_LESS - 75,  S_GREATER - 76
	K_IS,         		// 33 --for is null -- search_token should be null
	K_NOT,				// 14 --for is NOT null -- search token should be null */
	int i = 0;
	unsigned char* new_buffer = NULL;
	int matches = 0;
	new_buffer = (unsigned char*)calloc(1, rec_size * rec_cnt);

	if(!buffer){
		return NULL;
	}
	else{
		int j = 0, record_counter = 0, rows = 1, new_b_cnt = 0;
		while (i < (rec_cnt * rec_size) )
		{
			cd_entry  *col = NULL;
			j = i;
			int k, col_offset = 0;
			int record_counter = i;
			bool copy = false;
			for (k = 0, col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); 
					k < tab_entry->num_columns; k++, col++)
			{	//while there are columns in tpd
				if(col->col_id != col_to_search){
					j += col->col_len + 1;
				}
				else{
					unsigned char col_len = (unsigned char)buffer[j];
					int b = j + 1;
					if(search_token->tok_value == K_NULL)
					{//for nulls
						int zero = 0;
						switch(rel_op){
							case K_IS://is null match
								if(memcmp(&col_len, &zero, 1) == 0){
									matches++;
									copy = true;							
								}
								break;
							case K_NOT://not null match
								if(memcmp(&col_len, &zero, 1) > 0){
									matches++;
									copy = true;
								}
								break;
						}						
					}
					else if (search_token->tok_value == INT_LITERAL){//for int data value
						int elem;
						char *int_b;
						int_b = (char*)calloc(1, sizeof(int));
						for (int a = 0; a < sizeof(int); a++)
							int_b[a] = buffer[b + a];
						memcpy(&elem, int_b, sizeof(int));
						switch(rel_op)
						{
							case S_EQUAL:
								if(atoi(search_token->tok_string) == elem){
									copy = true;
									matches++;
								}
								break;
							case S_LESS:
								if(elem < atoi(search_token->tok_string)){
									copy = true;
									matches++;
								}
								break;
							case S_GREATER:
								if(elem > atoi(search_token->tok_string)){
									copy = true;
									matches++;
								}
								break;
						}//end rel_operator switch	
					}
					else if (search_token->tok_value == STRING_LITERAL){//for char data value
						char *str_b;
						int len = col->col_len + 1;
						str_b = (char*)calloc(1, len);
						for (int a = 0; a < len; a++)
							str_b[a] = buffer[b + a];
						str_b[len - 1] = '\0';
						switch(rel_op)
						{
							case S_EQUAL:
								if (memcmp(search_token->tok_string, str_b, col->col_len) == 0){
									matches++;
									copy = true;
								}
								break;
							case S_LESS:
								if (memcmp(search_token->tok_string, str_b, col->col_len) > 0){
									matches++;
									copy = true;
								}
								break;
							case S_GREATER:
								if (memcmp(search_token->tok_string, str_b, col->col_len) < 0){
									matches++;
									copy = true;
								}
								break;
						}		
					}
				}
			}
			if(copy){//if the row matches the condition, copy it to the buffer
				for(int a = 0; a < rec_size; a++){
					new_buffer[new_b_cnt++] = buffer[i + a];
				}
			}
			record_counter += rec_size; //jump to next record
			i = record_counter;
			rows++;
		}
		unsigned char* temp_buffer = NULL;
		temp_buffer = (unsigned char*)calloc(1, rec_size * matches);

		for(int k = 0; k < (rec_size*matches); k++){
			temp_buffer[k] = new_buffer[k];
		}
		return temp_buffer;
	}
}

unsigned char* selectRowsForValueOr(unsigned char* buffer, tpd_entry *tab_entry, int col_to_search, token_list *search_token, int rel_op, int rec_cnt, int rec_size, int col_to_search2, token_list *search_token2, int rel_op2)
{	//return a string buffer of rows that match the OR where condition in select statments
	/*possible values of rel_op: 
	S_EQUAL - 74, S_LESS- 75, S_GREATER- 76
	K_IS,         		// 33 --for is null -- search_token should be null
	K_NOT,				// 14 --for is NOT null -- search token should be null */
	int i = 0;
	unsigned char* new_buffer = NULL;
	int matches = 0;
	new_buffer = (unsigned char*)calloc(1, rec_size * rec_cnt);

	if(!buffer){
		return NULL;
	}
	else{
		int j = 0, record_counter = 0, rows = 1, new_b_cnt = 0;
		while (i < (rec_cnt * rec_size) )
		{
			cd_entry  *col = NULL;
			j = i;
			int k, col_offset = 0;
			int record_counter = i;
			bool copy = false;
			for (k = 0, col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); 
					k < tab_entry->num_columns; k++, col++)
			{	//while there are columns in tpd
				if(col->col_id != col_to_search){
					j += col->col_len + 1;
				}//continue until column # matches
				else{
					unsigned char col_len = (unsigned char)buffer[j];
					int b = j + 1;
					if(search_token->tok_value == K_NULL)
					{//for nulls
						int zero = 0;
						switch(rel_op){
							case K_IS://is null match
								if(memcmp(&col_len, &zero, 1) == 0){
									matches++;
									copy = true;							
								}
								break;
							case K_NOT://not null match
								if(memcmp(&col_len, &zero, 1) > 0){
									matches++;
									copy = true;
								}
								break;
						}						
					}
					else if (search_token->tok_value == INT_LITERAL){//for int data values
						int elem;
						char *int_b;
						int_b = (char*)calloc(1, sizeof(int));
						for (int a = 0; a < sizeof(int); a++)
							int_b[a] = buffer[b + a];
						memcpy(&elem, int_b, sizeof(int));
						switch(rel_op)
						{
							case S_EQUAL:
								if(atoi(search_token->tok_string) == elem){
									copy = true;
									matches++;
								}
								break;
							case S_LESS:
								if(elem < atoi(search_token->tok_string)){
									copy = true;
									matches++;
								}
								break;
							case S_GREATER:
								if(elem > atoi(search_token->tok_string)){
									copy = true;
									matches++;
								}
								break;
						}
					}
					else if (search_token->tok_value == STRING_LITERAL){//for char data values
						char *str_b;
						int len = col->col_len + 1;
						str_b = (char*)calloc(1, len);
						for (int a = 0; a < len; a++)
							str_b[a] = buffer[b + a];
						str_b[len - 1] = '\0';
						switch(rel_op)
						{
							case S_EQUAL:
								if (memcmp(search_token->tok_string, str_b, col->col_len) == 0){
									matches++;
									copy = true;
								}
								break;
							case S_LESS:
								if (memcmp(search_token->tok_string, str_b, col->col_len) > 0){
									matches++;
									copy = true;
								}
								break;
							case S_GREATER:
								if (memcmp(search_token->tok_string, str_b, col->col_len) < 0){
									matches++;
									copy = true;
								}
								break;
						}		
					}
				}
			}
			
			if(copy){//if row matches the first where condition, copy it to the buffer
				for(int a = 0; a < rec_size; a++){
					new_buffer[new_b_cnt++] = buffer[i + a];
				}
			}
			else{ //not match 1st condition, try going through again using 2nd condition (since it is OR)
				cd_entry  *col = NULL;
				j = i;
				int k, col_offset = 0;
				int record_counter = i;
				bool copy2 = false;
				for (k = 0, col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); 
					k < tab_entry->num_columns; k++, col++)
				{	//while there are columns in tpd
					//unsigned char col_len == (unsigned char)buffer[record_counter];
					if(col->col_id != col_to_search2){
						j += col->col_len + 1;
					}//continue until get to matching col
					else{
						unsigned char col_len = (unsigned char)buffer[j];
						int b = j + 1;
						if(search_token2->tok_value == K_NULL)
						{//for nulls
							int zero = 0;
							switch(rel_op2){
								case K_IS://is null condition
									if(memcmp(&col_len, &zero, 1) == 0){
										matches++;
										copy2 = true;						
									}
									break;
								case K_NOT://is not null condition
									if(memcmp(&col_len, &zero, 1) > 0){
										matches++;
										copy2 = true;
									}
									break;
							}						
						}
						else if (search_token2->tok_value == INT_LITERAL){//for int values
							int elem;
							char *int_b;
							int_b = (char*)calloc(1, sizeof(int));
							for (int a = 0; a < sizeof(int); a++)
								int_b[a] = buffer[b + a];
							memcpy(&elem, int_b, sizeof(int));
							switch(rel_op2)
							{
								case S_EQUAL:
									if(atoi(search_token2->tok_string) == elem){
										copy2 = true;
										matches++;
									}
									break;
								case S_LESS:
									if(elem < atoi(search_token2->tok_string)){
										copy2 = true;
										matches++;
									}
									break;
								case S_GREATER:
									if(elem > atoi(search_token2->tok_string)){
										copy2 = true;
										matches++;
									}
									break;
							}
						}
						else if (search_token2->tok_value == STRING_LITERAL){//for char data values
							char *str_b;
							int len = col->col_len + 1;
							str_b = (char*)calloc(1, len);
							for (int a = 0; a < len; a++)
								str_b[a] = buffer[b + a];
							str_b[len - 1] = '\0';
							switch(rel_op2)
							{
								case S_EQUAL:
									if (memcmp(search_token2->tok_string, str_b, col->col_len) == 0){
										matches++;
										copy2 = true;
									}
									break;
								case S_LESS:
									if (memcmp(search_token2->tok_string, str_b, col->col_len) > 0){
										matches++;
										copy2 = true;
									}
									break;
								case S_GREATER:
									if (memcmp(search_token2->tok_string, str_b, col->col_len) < 0){
										matches++;
										copy2 = true;
									}
									break;
							}//end switch		
						}
					}
				}

				if(copy2){//if matches second where clause (in OR), copy it
					for(int a = 0; a < rec_size; a++){
						new_buffer[new_b_cnt++] = buffer[i + a];
					}
				}
			}
			record_counter += rec_size; //jump to next record
			i = record_counter;
			rows++;
		}
		//create new buffer of size of the matching rows only
		unsigned char* temp_buffer = NULL;
		temp_buffer = (unsigned char*)calloc(1, rec_size * matches);
		for(int k = 0; k < (rec_size*matches); k++){
			temp_buffer[k] = new_buffer[k];
		}
		return temp_buffer;
	}
}

int getNumberOfMatches(unsigned char* buffer, tpd_entry *tab_entry, int col_to_search, token_list *search_token, int rel_op, int rec_cnt, int rec_size)
{	//calculates number of matches
	int i = 0, matches = 0;
	if(!buffer){
		return -1;
	}
	else{
		int j = 0, record_counter = 0, rows = 1, new_b_cnt = 0;
		while (i < (rec_cnt * rec_size) )
		{
			cd_entry  *col = NULL;
			j = i;
			int k, col_offset = 0;
			int record_counter = i;
			for (k = 0, col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); 
					k < tab_entry->num_columns; k++, col++)
			{	//while there are columns in tpd
				if(col->col_id != col_to_search){
					j += col->col_len + 1;
				}//continue until col matches
				else{
					unsigned char col_len = (unsigned char)buffer[j];
					int b = j + 1;
					if(search_token->tok_value == K_NULL)
					{//for nulls
						int zero = 0;
						switch(rel_op){
							case K_IS:
								if(memcmp(&col_len, &zero, 1) == 0)
									matches++;								
								break;
							case K_NOT:
								if(memcmp(&col_len, &zero, 1) > 0)
									matches++;
								break;
						}						
					}
					else if (search_token->tok_value == INT_LITERAL){
						bool copy = false;
						int elem;
						char *int_b;
						int_b = (char*)calloc(1, sizeof(int));
						for (int a = 0; a < sizeof(int); a++)
							int_b[a] = buffer[b + a];
						memcpy(&elem, int_b, sizeof(int));
						switch(rel_op)
						{
							case S_EQUAL:
								if(atoi(search_token->tok_string) == elem)
									matches++;
								break;
							case S_LESS:
								if(elem < atoi(search_token->tok_string))
									matches++;
								break;
							case S_GREATER:
								if(elem > atoi(search_token->tok_string))
									matches++;
								break;
						}
					}
					else if (search_token->tok_value == STRING_LITERAL){
						bool copy = false;
						char *str_b;
						int len = col->col_len + 1;
						str_b = (char*)calloc(1, len);
						for (int a = 0; a < len; a++)
						{
							str_b[a] = buffer[b + a];
						}
						str_b[len - 1] = '\0';
						switch(rel_op)
						{
							case S_EQUAL:
								if (memcmp(search_token->tok_string, str_b, col->col_len) == 0)
									matches++;
								break;
							case S_LESS:
								if (memcmp(search_token->tok_string, str_b, col->col_len) > 0)
									matches++;
								break;
							case S_GREATER:
								if (memcmp(search_token->tok_string, str_b, col->col_len) < 0)
									matches++;
								break;
						}		
					}
				}
			}
			record_counter += rec_size; //jump to next record
			i = record_counter;
			rows++;
		}
		return matches;
	}
}

int getNumberOfMatchesOr(unsigned char* buffer, tpd_entry *tab_entry, int col_to_search, token_list *search_token, int rel_op, int rec_cnt, int rec_size, int col_to_search2, token_list *search_token2, int rel_op2)
{
	/*possible values of rel_op: 
	S_EQUAL,            // 74
	S_LESS,             // 75
	S_GREATER,          // 76
	K_IS,         		// 33 --for is null -- search_token should be null
	K_NOT,				// 14 --for is NOT null -- search token should be null */
	int i = 0;
	unsigned char* new_buffer = NULL;
	int matches = 0;

	//printf("length of the rows is %d\n", rec_size * rec_cnt);
	new_buffer = (unsigned char*)calloc(1, rec_size * rec_cnt);

	if(!buffer){
		return NULL;
	}
	else{
		int j = 0, record_counter = 0, rows = 1, new_b_cnt = 0;
		while (i < (rec_cnt * rec_size) )
		{
			cd_entry  *col = NULL;
			j = i;
			int k, col_offset = 0;
			int record_counter = i;
			bool copy = false;
			for (k = 0, col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); 
					k < tab_entry->num_columns; k++, col++)
			{	//while there are columns in tpd
				//unsigned char col_len == (unsigned char)buffer[record_counter];
				if(col->col_id != col_to_search){
					j += col->col_len + 1;
					//printf("col not match. continue\n");
				}
				else{
					unsigned char col_len = (unsigned char)buffer[j];
					//printf("col_len is %u\n", col_len);
					//printf("search_token tok_value is %d\n", search_token->tok_value);
					//printf("rel_op is %d\n", rel_op);
					int b = j + 1;
					if(search_token->tok_value == K_NULL)
					{//for nulls
						int zero = 0;
						switch(rel_op){
							case K_IS:
								if(memcmp(&col_len, &zero, 1) == 0)
								{
									//printf("found a null match!\n");
									matches++;
									copy = true;
									/*for(int a = 0; a < rec_size; a++){
										new_buffer[new_b_cnt++] = buffer[j + a];
									}*/									
								}
								break;
							case K_NOT:
								if(memcmp(&col_len, &zero, 1) > 0)
								{
									//printf("found a not null match!\n");
									matches++;
									/*for(int a = 0; a < rec_size; a++){
										new_buffer[new_b_cnt++] = buffer[j + a];
									}*/
										copy = true;
								}
								break;
						}						
					}
					else if (search_token->tok_value == INT_LITERAL){
						
						int elem;
						char *int_b;
						int_b = (char*)calloc(1, sizeof(int));
						for (int a = 0; a < sizeof(int); a++)
						{
							int_b[a] = buffer[b + a];
						}
						memcpy(&elem, int_b, sizeof(int));
						//printf("element is %d\n",elem);
						switch(rel_op)
						{
							case S_EQUAL:
								if(atoi(search_token->tok_string) == elem)
								{
									//printf("found an = match\n");
									copy = true;
									matches++;
								}
								break;
							case S_LESS:
								if(elem < atoi(search_token->tok_string))
								{
									//printf("found an < match\n");
									copy = true;
									matches++;
								}
								break;
							case S_GREATER:
								if(elem > atoi(search_token->tok_string))
								{
									//printf("found an > match\n");
									copy = true;
									matches++;
								}
								break;
						}

						
					}
					else if (search_token->tok_value == STRING_LITERAL){
						char *str_b;
						int len = col->col_len + 1;
						str_b = (char*)calloc(1, len);
						for (int a = 0; a < len; a++)
						{
							str_b[a] = buffer[b + a];
						}
						str_b[len - 1] = '\0';
						//printf("%s\n", str_b);
						switch(rel_op)
						{
							case S_EQUAL:
								if (memcmp(search_token->tok_string, str_b, col->col_len) == 0)
								{
									matches++;
									//printf("         match cnt %d\n", matches);
									copy = true;
								}
								break;
							case S_LESS:
								if (memcmp(search_token->tok_string, str_b, col->col_len) > 0)
								{
									matches++;
									//printf("         match cnt %d\n", matches);
									copy = true;
								}
								break;
							case S_GREATER:
								if (memcmp(search_token->tok_string, str_b, col->col_len) < 0)
								{
									matches++;
									//printf("         match cnt %d\n", matches);
									copy = true;
								}
								break;
						}		
					}
				}
			}
			
			if(!copy){
				cd_entry  *col = NULL;
				j = i;
				int k, col_offset = 0;
				int record_counter = i;
				bool copy2 = false;
				for (k = 0, col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); 
					k < tab_entry->num_columns; k++, col++)
				{	//while there are columns in tpd
					//unsigned char col_len == (unsigned char)buffer[record_counter];
					if(col->col_id != col_to_search2){
						j += col->col_len + 1;
						//printf("col not match. continue\n");
					}
					else{
						unsigned char col_len = (unsigned char)buffer[j];
						//printf("col_len is %u\n", col_len);
						//printf("search_token tok_value is %d\n", search_token->tok_value);
						//printf("rel_op is %d\n", rel_op);
						int b = j + 1;
						if(search_token2->tok_value == K_NULL)
						{//for nulls
							int zero = 0;
							switch(rel_op2){
								case K_IS:
									if(memcmp(&col_len, &zero, 1) == 0)
									{
										//printf("found a null match!\n");
										matches++;
										copy = true;
										/*for(int a = 0; a < rec_size; a++){
											new_buffer[new_b_cnt++] = buffer[j + a];
										}*/									
									}
									break;
								case K_NOT:
									if(memcmp(&col_len, &zero, 1) > 0)
									{
										//printf("found a not null match!\n");
										matches++;
										/*for(int a = 0; a < rec_size; a++){
											new_buffer[new_b_cnt++] = buffer[j + a];
										}*/
											copy = true;
									}
									break;
							}						
						}
						else if (search_token2->tok_value == INT_LITERAL)
						{	
							int elem;
							char *int_b;
							int_b = (char*)calloc(1, sizeof(int));
							for (int a = 0; a < sizeof(int); a++)
							{
								int_b[a] = buffer[b + a];
							}
							memcpy(&elem, int_b, sizeof(int));
							//printf("element is %d\n",elem);
							switch(rel_op2)
							{
								case S_EQUAL:
									if(atoi(search_token2->tok_string) == elem)
									{
										//printf("found an = match\n");
										copy = true;
										matches++;
									}
									break;
								case S_LESS:
									if(elem < atoi(search_token2->tok_string))
									{
										//printf("found an < match\n");
										copy = true;
										matches++;
									}
									break;
								case S_GREATER:
									if(elem > atoi(search_token2->tok_string))
									{
										//printf("found an > match\n");
										copy = true;
										matches++;
									}
									break;
							}
						}
						else if (search_token2->tok_value == STRING_LITERAL){
							char *str_b;
							int len = col->col_len + 1;
							str_b = (char*)calloc(1, len);
							for (int a = 0; a < len; a++)
							{
								str_b[a] = buffer[b + a];
							}
							str_b[len - 1] = '\0';
							//printf("%s\n", str_b);
							switch(rel_op2)
							{
								case S_EQUAL:
									if (memcmp(search_token2->tok_string, str_b, col->col_len) == 0)
									{
										matches++;
										//printf("         match cnt %d\n", matches);
										copy = true;
									}
									break;
								case S_LESS:
									if (memcmp(search_token2->tok_string, str_b, col->col_len) > 0)
									{
										matches++;
										//printf("         match cnt %d\n", matches);
										copy = true;
									}
									break;
								case S_GREATER:
									if (memcmp(search_token2->tok_string, str_b, col->col_len) < 0)
									{
										matches++;
										//printf("         match cnt %d\n", matches);
										copy = true;
									}
									break;
							}//end switch		
						}
					}
				}
			}

			record_counter += rec_size; //jump to next record
			i = record_counter;
			rows++;
		}

		//printf("matching rows is %d\n\n", matches);

		return matches;
	}
}

table_file_header* getTFH(tpd_entry *tab_entry)
{
	struct _stat file_stat;
	table_file_header *recs = NULL;

	/* read from <table_name>.tab file*/
	char str[MAX_IDENT_LEN];
	strcpy(str, tab_entry->table_name); //get table name
	strcat(str, ".tab");

	FILE *fhandle = NULL;
	if ((fhandle = fopen(str, "rbc")) == NULL)
	{
		printf("there was an error opening the file %s\n",str);
		return NULL;
	}//end file open error
	else
	{
		_fstat(_fileno(fhandle), &file_stat);
		recs = (table_file_header*)calloc(1, file_stat.st_size);

		if (!recs)
		{
			printf("there was a memory error...\n");
			return NULL;
		}//end memory error
		else
		{
			fread(recs, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			
			if (recs->file_size != file_stat.st_size)
			{
				printf("ptr file_size: %d\n", recs->file_size);
				printf("read file size and calculated size does not match. db file is corrupted. exiting...\n");
				return NULL;
			}
			else
			{
				return recs;
			}
		}
	}
}

unsigned char* orderByBuffer(unsigned char* buffer, tpd_entry *tab_entry, int col_to_order_on, int order, int rec_size, int rec_cnt)
{
	//order - 32 is desc, 1 for default asc
	int i = 0;
	unsigned char* new_buffer = NULL;
	unsigned char* temp_buffer = NULL;
	int matches = 0;

	//printf("length of the rows is %d\n", rec_size * rec_cnt);
	new_buffer = (unsigned char*)calloc(1, rec_size * rec_cnt);
	temp_buffer = (unsigned char*)calloc(1, rec_size); //hold temp record being swapped

	if(!buffer){
		return NULL;
	}
	else{
		int j = 0, record_counter = 0, rows = 1, inner_row_cnt = 0, new_b_cnt = 0;
		//printf("rec_cnt is %d and rec_size is %d so total is %d\n", rec_cnt, rec_size, rec_cnt*rec_size);

		while (i < (rec_cnt * rec_size) )
		{
			//j = i;
			inner_row_cnt = 0, j = 0;
			int index = 0;
			while(inner_row_cnt < rec_cnt-1){
				
				cd_entry  *col = NULL;
				int k, col_offset = 0;
				int record_counter = i;
				bool copy = false;

				for (k = 0, col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); 
					k < tab_entry->num_columns; k++, col++)
				{	//while there are columns in tpd
					//unsigned char col_len == (unsigned char)buffer[record_counter];
					if(col->col_id != col_to_order_on){
						j += col->col_len + 1;
						//printf("col not match. continue\n");
					}
					else{
						unsigned char col_len = (unsigned char)buffer[j];
						//printf("col_len is %u\n", col_len);
						//printf("search_token tok_value is %d\n", search_token->tok_value);
						//printf("rel_op is %d\n", rel_op);
						int b = j + 1;
						if (col->col_type == T_INT){
							int elem;
							char *int_b;
							int_b = (char*)calloc(1, sizeof(int));

							if((int)col_len == 0)
							{//for nulls
								//printf("is null - order to bottom\n");
								elem = -99;
							}
							else{
								for (int a = 0; a < sizeof(int); a++)
								{
									int_b[a] = buffer[b + a];
								}
								memcpy(&elem, int_b, sizeof(int));
							}
							//printf("element is %d\n",elem);

							//compare to next item
							int elem2;
							char *int_b2;
							int_b2 = (char*)calloc(1, sizeof(int));
							for (int a = 0; a < sizeof(int); a++)
							{
								int_b2[a] = buffer[b + a + rec_size];
							}
							memcpy(&elem2, int_b2, sizeof(int));
							//printf("  element2 is %d\n",elem2);

							switch(order){
								case 1: //for asc (default)
									if(elem > elem2){
										for(int a = 0; a < rec_size; a++){
											temp_buffer[a] = buffer[index + a];
											buffer[index + a] = buffer[index + a + rec_size];
											buffer[index + a + rec_size] = temp_buffer[a];
										}
									}
									break;
								case 32: //for desc
									if(elem < elem2){
										for(int a = 0; a < rec_size; a++){
											temp_buffer[a] = buffer[index + a];
											buffer[index+ a] = buffer[index + a + rec_size];
											buffer[index + a + rec_size] = temp_buffer[a];
										}
									}
									break;
							}
						}
						else if (col->col_type == T_CHAR){
							char *str_b;
							int len = col->col_len + 1;
							str_b = (char*)calloc(1, len);
							for (int a = 0; a < len; a++)
							{
								str_b[a] = buffer[b + a];
							}
							str_b[len - 1] = '\0';
							//printf("first: %s\n", str_b);

							//compare to next item
							char *str_b2;
							len = col->col_len + 1;
							str_b2 = (char*)calloc(1, len);
							for (int a = 0; a < len; a++)
							{
								str_b2[a] = buffer[b + a + rec_size];
							}
							str_b2[len - 1] = '\0';
							//printf("  second: %s\n", str_b2);

							switch(order){
								case 1: //for asc (default)
									if(strcmp(str_b,str_b2) > 0){
										for(int a = 0; a < rec_size; a++){
											temp_buffer[a] = buffer[index + a];
											buffer[index + a] = buffer[index + a + rec_size];
											buffer[index + a + rec_size] = temp_buffer[a];
										}
									}
									break;
								case 32: //for desc
									if(strcmp(str_b,str_b2) < 0){
										for(int a = 0; a < rec_size; a++){
											temp_buffer[a] = buffer[index + a];
											buffer[index + a] = buffer[index + a + rec_size];
											buffer[index + a + rec_size] = temp_buffer[a];
										}
									}
									break;
							}							

						}
						break;
					}
				}
				//printf("in here at record # %d\n", rows);

				/*if(copy){
					for(int a = 0; a < rec_size; a++){
						new_buffer[new_b_cnt++] = buffer[i + a];
					}
				}*/
				//printf("  in inner loop at record offset %d (row #%d)\n", j, inner_row_cnt);
				index += rec_size;
				j = index;
				inner_row_cnt++;
			}
			rows++;
			//printf("in outer loop at record offset %d\n", i);
			record_counter += rec_size; //jump to next record
			i = record_counter;
		}


		return buffer;
	}
}

int restoreHelper(token_list *img_file_name)
{
	int rc = 0;
	FILE *fhandle = NULL;
	FILE *fdelete = NULL;
	struct _stat file_stat;

	if((fhandle = fopen(img_file_name->tok_string, "rbc")) != NULL)
	{
		//get size of dbfile.bin
		char *file_size_buf;
		int file_size;
		file_size_buf = (char*)calloc(1, sizeof(int));
		for(int a = 0; a < sizeof(int); a++){
			fread(&file_size_buf[a], 1, 1, fhandle);
		}
		memcpy(&file_size, file_size_buf, sizeof(int));
		//printf("dbfile.bin size is %d\n", file_size);

		tpd_list *dbfile_contents = (tpd_list*)calloc(1, file_size);
		fread(dbfile_contents, file_size, 1, fhandle);

		//restore dbfile.bin
		if((fdelete = fopen("dbfile.bin", "wbc")) != NULL){
			fwrite(dbfile_contents, file_size, 1, fdelete);
			fflush(fdelete);
			fclose(fdelete);
			printf("Restoring dbfile.bin\n");
			rc = initialize_tpd_list(); //restores g_tpd_list
			if (rc)
			{
				printf("\nError in initialize_tpd_list().\nrc = %d\n", rc);
				return rc;
			}
		}
		else{
			rc = FILE_OPEN_ERROR;
		}

		int num_tables = g_tpd_list->num_tables;
		tpd_entry *cur = &(g_tpd_list->tpd_start);
		while(!feof(fhandle))
		{
			if(num_tables>0){
				//get next file size
				char *file_size_buf;
				int file_size;
				file_size_buf = (char*)calloc(1, sizeof(int));
				for(int a = 0; a < sizeof(int); a++){
					fread(&file_size_buf[a], 1, 1, fhandle);
				}
				memcpy(&file_size, file_size_buf, sizeof(int));
				//printf("%s size is %d\n", cur->table_name, file_size);

				//get xx.tab file name
				char *filename = (char*)calloc(1,strlen(cur->table_name)+4);
				strcat(filename, cur->table_name);
				strcat(filename,".tab");
				printf("Restoring %s\n",filename);

				//get contents
				table_file_header *file_contents = (table_file_header*)calloc(1, file_size);
				fread(file_contents, file_size, 1, fhandle);

				//restore xx.tab file
				if((fdelete = fopen(filename, "wbc")) != NULL){
					fwrite(file_contents, file_size, 1, fdelete);
					fflush(fdelete);
					fclose(fdelete);
				}
				else{
					rc = FILE_OPEN_ERROR;
				}

				num_tables--;
				cur = (tpd_entry*)((char*)cur + cur->tpd_size);
			}	
			else
				break;
		}
	}
	else{
		printf("error opening %s\n", img_file_name->tok_string);
		rc = FILE_OPEN_ERROR;
	}

	return rc;
}

int pruneLog(token_list *img_file_name, bool without_rf)
{
	int rc = 0;
	FILE *fhandle = NULL;
	FILE *fwriter = NULL;
	FILE *flogger = NULL;
	char ts[16];
	char *now = get_timestamp();
	char cmd[MAX_PRINT_LEN];
	char *toCompare = (char*)calloc(1,MAX_PRINT_LEN);
	char bkup[MAX_PRINT_LEN];
	char *existing_log = NULL;
	long offset = 0;
	struct _stat file_stat;
	char *temp = (char*)calloc(1,sizeof(int));
	//ts = get_timestamp();
	//printf("ts: %s\n",ts);

	strcat(toCompare,"\"backup to ");
	strcat(toCompare, img_file_name->tok_string);
	strcat(toCompare, "\"");
	//printf("the test string to find is %s\n", toCompare);

	if((fhandle = fopen("db.log", "r")) != NULL)
	{
		_fstat(_fileno(fhandle), &file_stat);
		existing_log = (char*)calloc(1, file_stat.st_size);
		while(!feof(fhandle))
		{
			if(fgets(ts,16,fhandle) != NULL){
				strcat(existing_log,ts);
				//printf("ts is %s, ", ts);
			}
			//fseek(fhandle, 1, SEEK_CUR);
			if(fgets(cmd,MAX_PRINT_LEN,fhandle) != NULL){
				strcat(existing_log, cmd);
				//printf("cmd is %s", cmd);
			}

			if(strncmp(toCompare, cmd, strlen(toCompare)) == 0){
				//printf("*****INSERT RF_START here*********\n");
				//printf("cur ts is %s\n",now);
				offset = ftell(fhandle);
				//printf("offset is %d\n",offset);

				break;
			}

		}

		//printf("existing log:\n%s\n", existing_log);
		//printf("without rf is %d\n", without_rf);

		char *prune = (char*)calloc(1,file_stat.st_size - offset);
		while(!feof(fhandle)){
			if(fgets(bkup,MAX_PRINT_LEN,fhandle) != NULL){
				//printf("the next line is %s\n",bkup);
				strcat(prune,bkup);
			}
		}
		printf("prune is:\n%s\n", prune);

		if(without_rf){
			//overwrite with pruned log
			if((fwriter = fopen("db.log", "w")) != NULL)
			{
				fprintf(fwriter, existing_log);
				fclose(fwriter);
				
				//need to add back pruned portion to a diff log file
				if(file_stat.st_size - offset > 0)
				{
					printf("without rf specified. pruning log file\n");
					int i = 1;
					itoa(i, temp, 10);
					char *log_name = (char*)calloc(1,6+sizeof(int));
					strcat(log_name, "db.log");
					strcat(log_name, temp);

					//printf("log_name to create is %s\n", log_name);

					while((flogger = fopen(log_name, "r")) != NULL)
					{
						i++;
						//printf("i is now %d\n", i);
						memset(log_name, 0, 6+sizeof(int));
						memset(temp, 0, sizeof(int));
						strcat(log_name, "db.log");
						itoa(i, temp, 10);
						//printf("temp is %s\n", temp);
						strcat(log_name, temp);
						//printf("logname is now %s\n", log_name);
					}

					printf("original log file contents saved to %s\n", log_name);
					if((flogger = fopen(log_name, "w")) != NULL){
						fprintf(flogger, existing_log);
						fprintf(flogger, prune);
					}
					else{
						printf("error opening log file to prune\n");
						rc = FILE_OPEN_ERROR;
					}
				}
			}
			else{
				printf("error reading from log file\n");
				rc = FILE_OPEN_ERROR;
			}
		}
		else{
			if((fwriter = fopen("db.log", "w")) != NULL){
				fprintf(fwriter, existing_log);
				fprintf(fwriter, now);
				fprintf(fwriter, " \"RF_START\"\n");
				fprintf(fwriter, prune);
				fflush(fwriter);
				fclose(fwriter);

				//set flag in g_tpd_list
				g_tpd_list->db_flags = ROLLFORWARD_PENDING;
				printf("db_flags is %d\n", g_tpd_list->db_flags);
				//getch();

				if((fwriter = fopen("dbfile.bin", "rbc")) != NULL)
				{
					if((fwriter = fopen("dbfile.bin", "wbc")) != NULL)
					{
						printf("g_tpd_list has size %d\n", g_tpd_list->list_size);
						fwrite(g_tpd_list, g_tpd_list->list_size, 1, fwriter);
						fflush(fwriter);
						fclose(fwriter);
					}
					else{
						rc = FILE_OPEN_ERROR;
					}
				}
				else{
					rc = FILE_OPEN_ERROR;
				}
			}
			else{
				printf("error opening log file\n");
				rc = FILE_OPEN_ERROR;
			}
		}

		fclose(fhandle);
	}
	else{
		printf("error reading from log file\n");
		rc = FILE_OPEN_ERROR;
	}

	return rc;

}