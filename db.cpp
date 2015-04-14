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
		rc = get_token(argv[1], &tok_list);

		/* Test code */
		tok_ptr = tok_list;
		while (tok_ptr != NULL)
		{
			//prints token string	token class		token value
						/*keyword = 1,	// 1
						identifier,		// 2
						symbol, 		// 3
						type_name,		// 4
						constant,		// 5
						function_name,	// 6
						terminator,		// 7
						error			// 8*/
			/*printf("%16s \t%d \t %d\n",tok_ptr->tok_string, tok_ptr->tok_class,
				      tok_ptr->tok_value);*/
			tok_ptr = tok_ptr->next;
		}
    
		if (!rc)
		{
			rc = do_semantic(tok_list);
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
		{	/* Find STRING_LITERRAL */
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
															if ((cur->tok_value != S_RIGHT_PAREN) &&															  (cur->tok_value != S_COMMA))
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
	
	//parse insert statement
	cur = t_list;
	
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
					
					if (cur->tok_value == S_COMMA)
						cur = cur->next;

					if (cur->tok_value == S_RIGHT_PAREN)
					{	//Error
						printf("number of columns and insert values don't match\n");
						rc = INSERT_VAL_COL_LEN_MISMATCH;
						cur->tok_value = INVALID;
						return rc;
					}
					else
					{	//else, if token list is not shorter than number of columns, continue parsing
						if ((cur->tok_value == INT_LITERAL) && (col_entry->col_type == T_INT))
						{	//for int literal

							//check that int literal is not greater than max int
							if (strlen(cur->tok_string) > 10)
							{
								printf("integer value error\n");
								rc = INSERT_INT_SIZE_FAILURE;
								cur->tok_value = INVALID;
								return rc;
							}
							else if ((strlen(cur->tok_string) == 10) && (strcmp(cur->tok_string, "2147483647") > 0))
							{
								printf("integer value error\n");
								rc = INSERT_INT_SIZE_FAILURE;
								cur->tok_value = INVALID;
								return rc;
							}
							else
							{
								//temporary token holder
								token_list *temp = (token_list *)calloc(1, sizeof(token_list));
								temp->tok_class = cur->tok_class;
								temp->tok_value = cur->tok_value;
								strcpy(temp->tok_string, cur->tok_string);
								temp->next = NULL;

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
								cur = cur->next;
							}
						}//end int literal
						else if ((cur->tok_value == STRING_LITERAL) && (col_entry->col_type == T_CHAR))
						{	//for char or string literal
							int str_size = strlen(cur->tok_string);

							if (str_size <= col_entry->col_len)
							{
								//temporary token holder
								token_list *temp = (token_list *)calloc(1, sizeof(token_list));
								//temp->tok_class = cur->tok_class; 
								temp->tok_class = col_entry->col_len; //hack to keep col max len
								temp->tok_value = cur->tok_value;
								strcpy(temp->tok_string, cur->tok_string);
								temp->next = NULL;
								
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

								//continue to next token
								cur = cur->next;
								
							}
							else
							{	//str size too long
								printf("char length error\n");
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
							else
							{	//nullable

								//temporary token holder
								token_list *temp = (token_list *)calloc(1, sizeof(token_list));
								//temp->tok_class = cur->tok_class; //1 for keyword
								//hack: set tok_class to hold column_len
								temp->tok_class = col_entry->col_len;
								temp->tok_value = cur->tok_value; //15 for null keyword
								strcpy(temp->tok_string, cur->tok_string);
								temp->next = NULL;

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
								
								//continue to next token
								cur = cur->next;
							}
						}//end null
						else
						{
							printf("type mismatch\n");
							rc = INSERT_VALUE_TYPE_MISMATCH;
							cur->tok_value = INVALID;
							return rc;
						}
					}//if not EOC
				}//end for

				if (cur->tok_value == S_RIGHT_PAREN)
				{
					//got R parentheses
					cur = cur->next;
				}
				else 
				{
					printf("missing ) for insert statement\n");
					rc = INVALID_INSERT_STATEMENT;
					return rc;
				}
			}//end else col/val comparison
		}//end else at L parentheses
	}//end else where table name found

	//do insert (write to .tab)
	if (!rc)
	{	//col/values match tpd columns definitions
		if(cur->tok_value == EOC)
		{
			// Write to <table_name>.tab file
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
								}
								
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
		{
			// there is extra stuff after R parantheses or extra values given
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
	struct _stat file_stat;
	table_file_header *recs = NULL;
	token_list *values = NULL, *head = NULL; //to hold retrieved token list

	cur = t_list;
	if ((cur->tok_value != S_STAR) && (cur->tok_class != identifier) && (cur->tok_class != function_name))
	{	// Error
		printf("invalid select statement\n");
		rc = INVALID_SELECT_STATEMENT;
		cur->tok_value = INVALID;
	}
	else 
	{	//select stmt has *, identifer or aggregate func
		if (cur->tok_value == S_STAR)
		{	//do select * here
			cur = cur->next;
			if (cur->tok_value != K_FROM)
			{	// Error
				printf("missing 'from' keyword in select statement\n");
				rc = INVALID_SELECT_STATEMENT;
				cur->tok_value = INVALID;
			}
			else
			{	//'from' keyword found, move to next token
				cur = cur->next;
				if(cur->tok_value == EOC)
				{
					printf("no table name to select from given\n");
					rc = INVALID_SELECT_STATEMENT;
					cur->tok_value = INVALID;
				}
				else if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
				{
					printf("cannot select from nonexistent table\n");
					rc = TABLE_NOT_EXIST;
					cur->tok_value = INVALID;
				}
				else
				{
					cur = cur->next; //look for clause after table name
					if (cur->tok_value == EOC)
					{
						//do select * from table
						/* read from <table_name>.tab file*/
						char str[MAX_IDENT_LEN];
						strcpy(str, tab_entry->table_name); //get table name
						strcat(str, ".tab");

						//get column headers
						char format[MAX_PRINT_LEN], head[MAX_PRINT_LEN];
						strcpy(format, "+");
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
							for (int j = 0; j < width; j++)
								strcat(format, "-");
							strcat(format, "+");

							int diff = width - strlen(col_entry->col_name);
							strcat(head, col_entry->col_name);
							if (diff > 0)
							{
								for (int k = 0; k < diff; k++)
									strcat(head, " ");
							}
							strcat(head, "|");
						}

						printf("%s\n", format);
						printf("%s\n", head);
						printf("%s\n", format);

						FILE *fhandle = NULL;
						if ((fhandle = fopen(str, "rbc")) == NULL)
						{
							printf("there was an error opening the file %s\n",str);
							rc = FILE_OPEN_ERROR;
						}//end file open error
						else
						{
							_fstat(_fileno(fhandle), &file_stat);
							recs = (table_file_header*)calloc(1, file_stat.st_size);

							if (!recs)
							{
								printf("there was a memory error...\n");
								rc = MEMORY_ERROR;
							}//end memory error
							else
							{
								fread(recs, file_stat.st_size, 1, fhandle);
								fflush(fhandle);
								
								if (recs->file_size != file_stat.st_size)
								{
									printf("ptr file_size: %d\n", recs->file_size);
									printf("read file size and calculated size does not match. db file is corrupted. exiting...\n");
									rc = DBFILE_CORRUPTION;
									return rc;
								}
								else
								{
									int rows_tb_size = recs->file_size - recs->record_offset;
									int record_size = recs->record_size;
									int offset = recs->record_offset;
									int num_records = recs->num_records;

									fseek(fhandle, offset, SEEK_SET);
									unsigned char *buffer;
									buffer = (unsigned char*)calloc(1, record_size * 100);
									if (!buffer)
										rc = MEMORY_ERROR;
									else
									{
										fread(buffer, rows_tb_size, 1, fhandle);
										int i = 0, rec_cnt = 0, rows = 1;
										while (i < rows_tb_size)
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
														{
															int_b[a] = buffer[b + a];
														}
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
														{
															str_b[a] = buffer[b + a];
														}
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

														//print to L or R depending on col type
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
														rc = DBFILE_CORRUPTION; //if byte is anything else
												}
												i += col->col_len+1; //move to next item/column
											}
											printf("\n");
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
						}//not file open error
					}
					else if (cur->tok_value == K_WHERE)
					{
						if(cur->next->tok_value == EOC)
						{
							printf("improper use of 'where' keyword. a clause must follow\n");
							rc = INVALID_WHERE_CLAUSE_IN_SELECT;
							cur->tok_value = INVALID;
						}
						else
						{
							cur = cur->next;
							printf("where clauses not yet implemented\n");
						}
					}
					else if (cur->tok_value == K_ORDER)
					{
						cur = cur->next; //move to 'by' keyword
						if(cur->tok_value == K_BY)
						{
							if(cur->next->tok_value == EOC)
							{
								printf("improper use of 'order by' keywords. a column name must follow\n");
								rc = INVALID_ORDER_BY_CLAUSE_IN_SELECT;
								cur->tok_value = INVALID;
							}
							else
							{
								cur = cur->next; //move to column name
								printf("order by clauses not yet implemented\n");
							}
						}
						else
						{
							printf("improper use of 'order' keyword. it must be followed by keyword 'by'.\n");
							rc = INVALID_ORDER_BY_CLAUSE_IN_SELECT;
							cur->tok_value = INVALID;
						}
					}
					else
					{//other things follow the select statement
						printf("invalid select statement:\n  only a 'where' clause or 'order by' clause may follow the table name\n");
						rc = INVALID_SELECT_STATEMENT;
						cur->tok_value = INVALID;
					}
				}//not table not exist
			}//end 'from' keyword check
		}//select * stmt
		else if (cur->tok_class == function_name)
		{
			printf("found aggregate func: %s\n", cur->tok_string);
			printf("aggregation function not yet implemented");
		}//aggregate function in select
		else if (cur->tok_class == identifier)
		{
			printf("cur: %s\n", cur->tok_string);
			printf("column select not yet implemented");
		}//found identifier in select
		else
		{	//if not table name or not aggregate function name
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
	struct _stat file_stat;
	table_file_header *header = NULL;
	table_file_header *recs = NULL;
	token_list *values = NULL, *head = NULL; //to hold retrieved token list

	cur = t_list;
	cur = cur->next; //move past FROM keyword
	if ((cur->tok_class != keyword) && (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{ 	//Error
		printf("invalid delete statement\n");
		rc = INVALID_DELETE_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{ 	//identifier found - check that table exists
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
		{
			printf("cannot delete from nonexistent table\n");
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;
			if(cur->tok_value == K_WHERE)
			{
				if(cur->next->tok_value != EOC)
				{
					cur = cur->next;
					//get column names and check if cur->string matches
					cd_entry  *col_entry = NULL;
					int i, col_id = 0, col_type = 0;
					bool col_match = false;
					for (i = 0, col_entry = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset); i < tab_entry->num_columns; i++, col_entry++)
					{
						if(strcmp(col_entry->col_name, cur->tok_string) == 0)
						{
							col_match = true;
							col_id = col_entry->col_id; //get column number
							col_type = col_entry->col_type; //get column type
						}//end strcmp
					}//end for

					/*printf("your col name is: '%s'\n", cur->tok_string);
					printf("col match is (T/F): %d\n",col_match);
					printf("col id is %d\n", col_id);*/

					if(col_match)
					{	//continue parsing
						cur = cur->next;
						if (cur->tok_value == EOC)
						{
							printf("invalid where clause in delete statement.\n  column name must be followed by >, <, or =");
							rc = INVALID_WHERE_CALUSE_IN_DELETE;
							cur->tok_value = INVALID;
						}
						else if ((cur->tok_value != S_EQUAL) && (cur->tok_value != S_LESS) && (cur->tok_value != S_GREATER))
						{
							printf("invalid where clause in delete statement.\n column name must be followed by >, <, or =\n");
							rc = INVALID_REL_COMP_IN_WHERE_IN_DELETE;
							cur->tok_value = INVALID;
						}
						else
						{	//if not EOC but is = > <
							printf("relation operator is %s\n", cur->tok_string);
							cur = cur->next;
							if(cur->next->tok_value != EOC)
							{
								printf("delete statement currently only supports one where condition\n");
								rc = INVALID_DELETE_STATEMENT;
								cur->next->tok_value = INVALID;
							}
							else
							{
								if((cur->tok_value == INT_LITERAL) && (col_type == T_INT))
								{
									printf("int data value found\n");
									int col_value = atoi(cur->tok_string);

									//read records from file
									char str[MAX_IDENT_LEN];
									strcpy(str, tab_entry->table_name);
									strcat(str, ".tab");

									FILE *fhandle = NULL;
									if ((fhandle = fopen(str, "rbc")) == NULL)
									{
										printf("there was an error opening the file %s\n",str);
										rc = FILE_OPEN_ERROR;
									}//end file open error
									else
									{
										_fstat(_fileno(fhandle), &file_stat);
										recs = (table_file_header*)calloc(1, file_stat.st_size);
										if (!recs)
										{
											printf("there was a memory error...\n");
											rc = MEMORY_ERROR;
										}//end memory error
										else
										{
											fread(recs, file_stat.st_size, 1, fhandle);
											fflush(fhandle);
											if (recs->file_size != file_stat.st_size)
											{
												printf("ptr file_size: %d\n", recs->file_size);
												printf("read file size and calculated size does not match. db file is corrupted. exiting...\n");
												rc = DBFILE_CORRUPTION;
												return rc;
											}
											else
											{
												int rows_tb_size = recs->file_size - recs->record_offset;
												int record_size = recs->record_size;
												int offset = recs->record_offset;
												int num_records = recs->num_records;

												fseek(fhandle, offset, SEEK_SET);
												unsigned char *buffer;
												buffer = (unsigned char*)calloc(1, record_size * num_records);

												unsigned char *temp;
												temp = (unsigned char*)calloc(1, record_size * num_records);		

												if (!buffer)
													rc = MEMORY_ERROR;
												else
												{
													fread(buffer, rows_tb_size, 1, fhandle);
													fflush(fhandle);
													fseek(fhandle, offset, SEEK_SET);

													//int rowArray[num_records];

													int i = 0, rec_cnt = 0, rows = 1, matches = 0, keepers=0;
													while (i < rows_tb_size)
													{
														printf("moving to this index now %d\n",i);
														cd_entry  *col = NULL;
														int k, row_col = 0;
														for (k = 0, col = (cd_entry*)((char*)tab_entry + tab_entry->cd_offset);
															k < tab_entry->num_columns; k++, col++)
														{	//while there are columns in tpd
															unsigned char col_len = (unsigned char)buffer[i];
															if(k == col_id)
															{	//if it is the selected column
																if ((int)col_len > 0)
																{
																	int b = i + 1;
																	char *int_b;
																	int elem;
																	int_b = (char*)calloc(1, sizeof(int));
																	for (int a = 0; a < sizeof(int); a++)
																	{
																		int_b[a] = buffer[b + a];
																	}
																	memcpy(&elem, int_b, sizeof(int));
																	if(elem != col_value)
																	{
																		printf("|row #%d| %11d|\n",rows, elem);
																		printf("adding to temp: %d\n", keepers);
																		int adder = keepers*record_size;
																		printf("freading now and adding to temp at: %d\n", adder);
																		fread(temp+adder, record_size, 1, fhandle);
																		keepers++;
																	}
																	else
																	{	//match found
																		printf("|row #%d| %11d|delete match\n",rows, elem);
																		matches++;
																		//skip record and increment fhandle pointer by one record size
																		printf("skipping ahead now\n");
																		fseek(fhandle, record_size, SEEK_CUR);
																	}
																}
																else if((int)col_len == 0)
																{
																	int b = i + 1;
																	char *null_b;
																	int len = 11;
																	null_b = (char*)calloc(1, len);
																	strcat(null_b, "-");
																	int str_len = strlen(null_b);
																	int width = (str_len > len) ? str_len : len;

																	printf("|row #%d| ", rows);
																	while (str_len < width)
																	{
																		printf(" ");
																		str_len++;
																	}
																	printf("%s|is null\n", null_b);
																	fread(temp, record_size, 1, fhandle);
																}
																k++;
															}
															i += col->col_len+1; //move to next item/column	
														}
														rows++;
														rec_cnt += record_size; //jump to next record
														i = rec_cnt;

													}//end while
													if(matches == 0)
													{
														printf("error: no row with matching data value in that column to be deleted\n");
														rc = NO_ROWS_TO_DELETE;
														cur->tok_value = INVALID;
													}
													else
													{
														int to_keep = num_records - matches;
														printf("number to delete %d, and number to rewrite %d\n", matches, to_keep);
														
														int n = record_size * to_keep;
														printf("size of record_size is %d and n is : %d\n",record_size,n);
														//fwrite(newbuff, record_size*matches, 1, fhandle);
														for(int k = 0; k < n; k++)
														{
															printf("%u",(unsigned char*)temp[k]);
														}

														if ((fhandle = fopen(str, "wbc")) == NULL)
														{
															printf("there was an error opening the file %s\n",str);
															rc = FILE_OPEN_ERROR;
														}
														else
														{
															recs->num_records = to_keep;
															recs->file_size = to_keep * record_size + sizeof(table_file_header);
															fwrite(recs, sizeof(table_file_header), 1, fhandle);
															fwrite(temp, n, 1, fhandle);
														}
														
															
													}//there are matches
												}//not memory error
											}
										}

									}//file open ok
								}//int data value
								else if ((cur->tok_value == STRING_LITERAL) && (col_type == T_CHAR))
								{
									printf("char data value found\n");
								}//char data value
								else
								{
									printf("data value in where clause does not match type of column\n");
									rc = WHERE_VAL_COLTYPE_MISMATCH_IN_DELETE;
									cur->tok_value = INVALID;
								}//coltype and val type mismatch
							}//only one where condition, nothing else after it
						}//relational operator found
					}
					else
					{
						printf("no matching column in table to select for deletion\n");
						rc = INVALID_COLUMN_NAME;
						cur->tok_value = INVALID;
					}
				}//tokens after where found
				else
				{
					printf("improper use of the keyword 'where'. a clause must follow\n");
					rc = INVALID_WHERE_CALUSE_IN_DELETE;
					cur->tok_value = INVALID;
				}
			}//where keyword found
			else if(cur->tok_value == EOC)
			{
				char str[MAX_IDENT_LEN + 1];
				strcpy(str, tab_entry->table_name); //get table name
				strcat(str, ".tab");
				FILE *fhandle = NULL;

				if ((fhandle = fopen(str, "rbc")) == NULL)
					rc = FILE_OPEN_ERROR;
				else
				{
					_fstat(_fileno(fhandle), &file_stat);
					header = (table_file_header*)calloc(1, file_stat.st_size);
					if(!header)
						rc = MEMORY_ERROR;
					else
					{
						printf("open for read ok\n");
						fread(header, sizeof(table_file_header), 1, fhandle);
						fflush(fhandle);
						fclose(fhandle);

						printf("file size is %d\n", header->file_size);
						printf("record size is %d\n", header->record_size);
						printf("num_recs is %d\n", header->num_records);
						printf("rec offset is at %d\n", header->record_offset);

						header->num_records = 0; // update to zero records
						header->file_size = sizeof(table_file_header);

						printf("new file size is %d\n", header->file_size);
						printf("new num_recs is %d\n", header->num_records);
						
						if ((fhandle = fopen(str, "wbc")) == NULL)
							rc = FILE_OPEN_ERROR;
						else
						{
							fwrite(header, sizeof(table_file_header), 1, fhandle);
							fflush(fhandle);
							fclose(fhandle);
							printf("file write ok\n");
						}
					}//not memory error	
				}//file open OK
			}//delete all rows (no where clause)
			else 
			{
				printf("invalid delete statement\n");
				rc = INVALID_DELETE_STATEMENT;
				cur->tok_value = INVALID;
			}//invalid delete stmt
		}//end else for table exists
	}//end else for identifier found

	return rc;
}

int sem_update(token_list *t_list)
{
	int rc = 0;
	token_list *cur;
	tpd_entry *tab_entry = NULL;
	struct _stat file_stat;
	table_file_header *recs = NULL;
	token_list *values = NULL, *head = NULL; //to hold retrieved token list

	cur = t_list;
	if ((cur->tok_class != keyword) && (cur->tok_class != identifier) &&
			(cur->tok_class != type_name))
	{ 	//Error
		printf("invalid update statement\n");
		rc = INVALID_UPDATE_STATEMENT;
		cur->tok_value = INVALID;
	}
	else
	{
		//identifier found - check that table exists
		if ((tab_entry = get_tpd_from_list(cur->tok_string)) == NULL)
		{
			printf("cannot update nonexistent table\n");
			rc = TABLE_NOT_EXIST;
			cur->tok_value = INVALID;
		}
		else
		{
			cur = cur->next;
			if(cur->tok_value == K_SET)
			{
				printf("need to get column name for table %s\n", tab_entry->table_name);
			}
			else 
			{
				printf("invalid update statement. missing set clause\n");
				rc = INVALID_UPDATE_STATEMENT;
				cur->tok_value = INVALID;
			}
		}//end else for table exists
	}

	return rc;
}

/*table_file_header * getTable(char table_name[])
{
	int rc = 0;
	table_file_header *table_content = NULL;
	struct _stat file_stat;

	printf("in getTable\n");

	FILE *fhandle = NULL;
	if ((fhandle = fopen(table_name, "rbc")) == NULL)
	{
		printf("there was an error opening the file %s\n",table_name);
		rc = FILE_OPEN_ERROR;
		return 0;
	}//end file open error
	else
	{
		_fstat(_fileno(fhandle), &file_stat);
		table_content = (table_file_header*)calloc(1, file_stat.st_size);

		if (!table_name)
		{
			printf("there was a memory error...\n");
			rc = MEMORY_ERROR;
		}//end memory error
		else
		{
			fread(table_content, file_stat.st_size, 1, fhandle);
			fflush(fhandle);
			fclose(fhandle);
		}
	}

	return table_content;
}*/