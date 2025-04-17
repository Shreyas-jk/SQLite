#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdbool.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

//DEFINE STUFF (makes the code easer to read, or harder depending on the person
//its my code and it makes it easier for me to understand so deal with it
#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
#define TABLE_MAX_PAGES 100

// The buffer for inputs - holds the data that we will be mainuplating and understading
typedef struct {
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

//Makes the rest of the code much easier to understand -  more like english speach
//All the execute results 
typedef enum { EXECUTE_SUCCESS, EXECUTE_TABLE_FULL } ExecuteResult;

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_SYNTAX_ERROR,
    PREPARE_STRING_TOO_LONG,
    PREPARE_NEGATIVE_ID,
    PREPARE_UNRECOGNIZED_STATEMENT,
} PrepareResult;

typedef enum { STATEMENT_INSERT, STATEMENT_SELECT } StatementType;


//if I have to explain wtf a row and a table is imma crash out
typedef struct {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

typedef struct {
    int file_descriptor;
    uint32_t file_length;
    void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
    uint32_t num_rows;
    Pager* pager;
} Table;
//Becuase we are not using array, we need to be able to traverse and this gives us a way to 
//search for an element
//"but what if its an introvert" i dont care
typedef struct{
    Table* table;
    uint32_t row_num;
    bool end_of_table;
}Cursor;

//States what kind of command(Insert or Select) and then takes a row value to create a full statement
//Kinda like a who what where when how kinda situation
typedef struct {
    StatementType type;
    Row row_to_insert; //only used by insert statement
} Statement;

//all the table attributes and everything, just makes sure we dont leak any memory
const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
const uint32_t PAGE_SIZE = 4096;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;


// --- Function Prototypes ---
PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement);
PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement);
MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table);
ExecuteResult execute_insert(Statement* statement, Table* table);
ExecuteResult execute_select(Statement* statement, Table* table);
ExecuteResult execute_statement(Statement* statement, Table* table);
InputBuffer* new_input_buffer();
Table* db_open(const char* filename);
Pager* pager_open(const char* filename);
Cursor* table_start(Table* table);
Cursor* table_end(Table* table);
void db_close(Table* table);
void close_input_buffer(InputBuffer* input_buffer);
void print_prompt();
void read_input(InputBuffer* input_buffer);
void serialize_row(Row* source, void* destination);
void deserialize_row(void* source, Row* destination);
void* cursor_value(Cursor* cursor);
void free_table(Table* table);
void print_row(Row* row);
void* get_page(Pager* pager, uint32_t page_num);
void pager_flush(Pager* pager, uint32_t page_num, uint32_t size);
void cursor_advance(Cursor* cursor);

// This just makes the rest of the program so much easier to work through
// becuase I can't keep moving things around to make it compile
// also i hope I keep updating this 

//Functions are here
//frees up the pointers -> good for memory I think, probably
//(if anyone is reading this I know its good it just a joke)
void close_input_buffer(InputBuffer* input_buffer) {
    free(input_buffer->buffer);
    free(input_buffer);
}

//our lovwely promp... the foundation of this empire 
void print_prompt() { printf("SQL > "); }

//when u select the row, u gotta print the row
//NOTE TO SELF MAKE IT EASILY READABLE, even I am gettign confused sometimes idk why
void print_row(Row* row) {
    //ok i did it
    printf("(ID: %d, USERNAME: %s, EMAIL: %s)\n", row->id, row->username, row->email);
}

//so to place it into a page, you need to copy the memory of the pointer into a slab of around 255 bytes
//and place them back to back to create like a block of memory to store the information on a page
void serialize_row(Row* source, void* destination) {
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

//You then need to read the information -> basically 
//read input into row struct -> transform into a brick of memory and place into a page -> untransform it
//when we need it again 
void deserialize_row(void *source, Row* destination) {
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

void* cursor_value(Cursor* cursor) {
    uint32_t row_num = cursor->row_num;
    uint32_t page_num = row_num / ROWS_PER_PAGE;
    void* page = get_page(cursor->table->pager, page_num);
    uint32_t row_offset = row_num % ROWS_PER_PAGE;
    uint32_t byte_offset = row_offset * ROW_SIZE;
    return page + byte_offset;
}
//So earlier implementations of this made it so there was no way to recover old data bases
//after .exit
//however sql can mke files to hold the pages and everything
//so the database open and close and flusher allow us to take our databases and flush them to the disk
//after we are done with the prompt, late I might implement a .exit function that allows us to delete
//and not flush but as of right now ti will create a file that while us mortal human beings
//cannot read it allows us o access data bases after .exit
Table* db_open(const char* filename) {
    Pager* pager = pager_open(filename);
    uint32_t num_rows = pager->file_length / ROW_SIZE;

    Table* table = (Table*)malloc(sizeof(Table));
    table->pager = pager;
    table->num_rows = num_rows;
    return table;
}

Pager* pager_open(const char* filename){
    //this is a key part where we create a new file or open an existing file based on the file name
    //the open function allows us to open a file in read mode(cause we have to specify *eye roll*)
    //the o_creat si a great way for us to create a new file to flush to 
    //The last two are permission for the owner of the file to read and write
    // for all the specifics https://pubs.opengroup.org/onlinepubs/007904875/functions/open.html
    //oh it hyperlinks on vs, that cool beans
    int fd = open(filename,O_RDWR|O_CREAT,S_IWUSR|S_IRUSR);

    if (fd == -1) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);

    Pager* pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;

    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }

    return pager;
}

//need a new way to get the page and everything 
void* get_page(Pager* pager, uint32_t page_num){
    if (page_num > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if (pager->pages[page_num] == NULL) {
        void* page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;
    

        if (pager->file_length % PAGE_SIZE) {
            num_pages += 1;
        }
        
        if (page_num <= num_pages) {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;
    } 
    return pager->pages[page_num];
}

//so this is where we move everything to the disk
//kinda goes into some linux topics
void pager_flush(Pager* pager, uint32_t page_num, uint32_t size) {
    if (pager->pages[page_num] == NULL) {
        printf("Tried to flush null page.\n");
        exit(EXIT_FAILURE);
    }
    //this part is insanely important becuase lseek is a way for us to take a file
    //ie. pager->file_descriptor and basically say here is where the beginneing of the file is
    //i think, it works and thats what matters
    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    //can't find anything, spits out error disrespectfully in your face
    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], size);
    //again if for some reason the write isn't able to write anything, it will spit out an error
    if (bytes_written == -1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

//closes the data base and uses pager_flush to flush to memory
void db_close(Table* table) {
    Pager* pager = table->pager;
    uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;
    
    for (uint32_t i = 0; i < num_full_pages; i++) {
        if (pager->pages[i] == NULL) {
            continue;
        }
        pager_flush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }
}

//allocates the input buffer, just like creating a instance of a class
InputBuffer* new_input_buffer() {
    InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;

    return input_buffer;
}

//I hate reading, but I had no clue we could do exitfailure as exit, so fancy 
void read_input(InputBuffer* input_buffer) {
    ssize_t bytes_read =
        getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
}


MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table *table) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        db_close(table);
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

//This section was created after the next part but one of my friends 
//hit me in the head and told me to make it a function
//apperently they are 'good coding practice' like i care
//i do but its work...
//makes it harder to read i think ???!!
PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement){
    statement -> type = STATEMENT_INSERT;

    char* keyword = strtok(input_buffer->buffer, " ");
    char* id_string = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");

    if(id_string == NULL || username == NULL || email == NULL){
        return PREPARE_SYNTAX_ERROR;
    }
    
    int id = atoi(id_string);
    if(id < 0){
        return PREPARE_NEGATIVE_ID;
    }
    if(strlen(username) > COLUMN_USERNAME_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }
    if(strlen(email) > COLUMN_EMAIL_SIZE){
        return PREPARE_STRING_TOO_LONG;
    }
    
    statement -> row_to_insert.id = id;
    strcpy(statement -> row_to_insert.username, username);
    strcpy(statement -> row_to_insert.email, email);
    return PREPARE_SUCCESS;
}



PrepareResult prepare_statement(InputBuffer* input_buffer,Statement* statement) {
    //these two are the basic select and insert functions
    //all we are doing in saying we notice them and passing what type of argument we are working with 
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        return prepare_insert(input_buffer,statement);
    }
    if (strcmp(input_buffer->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

//testing for rn, but this is where the actualy executing of statments happens
ExecuteResult execute_insert(Statement* statement, Table* table) {
    if (table->num_rows >= TABLE_MAX_ROWS) {
    return EXECUTE_TABLE_FULL;
    }

    Row* row_to_insert = &(statement->row_to_insert);
    Cursor* cursor = table_end(table);

    serialize_row(row_to_insert, cursor_value(cursor));

    table->num_rows += 1;

    return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
    Cursor* cursor = table_start(table);

    Row row;
    while(!(cursor->end_of_table)){
        deserialize_row(cursor_value(cursor), &row);
        print_row(&row);
        cursor_advance(cursor);
    }
    free(cursor);
    return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, Table *table) {
    switch (statement->type) {
        case (STATEMENT_INSERT):
            return execute_insert(statement, table);
    case (STATEMENT_SELECT):
        return execute_select(statement, table);
    }
}
//creates a new cursor and initalizes it to the start of a specific table 
Cursor* table_start(Table* table) {
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num = 0;
    cursor->end_of_table = (table->num_rows == 0);

    return cursor;
}

//goes to the last element and pops in there
Cursor* table_end(Table* table) {
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->row_num = table->num_rows;
    cursor->end_of_table = true;

    return cursor;
}

//Don't ask you can figure it out
void cursor_advance(Cursor* cursor) {
    cursor->row_num += 1;
    if (cursor->row_num >= cursor->table->num_rows) {
        cursor->end_of_table = true;
    }
}





// -------MAIN FUNCTION----------
// in case if ur blind like me
int main(int argc, char* argv[]) {

    if (argc < 2) {
            printf("Must supply a database filename.\n");
            exit(EXIT_FAILURE);
    }
    char* filename = argv[1];
    Table* table = db_open(filename);

    InputBuffer* input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);
        if(input_buffer -> buffer[0] == '.'){
            switch (do_meta_command(input_buffer, table)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '%s'\n", input_buffer->buffer);
                    continue;
            }
        }

        Statement statement;
        switch (prepare_statement(input_buffer, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case (PREPARE_STRING_TOO_LONG):
                printf("String is too long.\n");
                continue;
            case (PREPARE_NEGATIVE_ID):
                printf("ID must be positive.\n");
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'.\n",input_buffer->buffer);
                continue;
        }

        switch (execute_statement(&statement, table)) {
            case (EXECUTE_SUCCESS):
                printf("Executed.\n");
                break;
            case (EXECUTE_TABLE_FULL):
                printf("Error: Table full.\n");
                break;
        }
    }
}