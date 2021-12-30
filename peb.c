/*
  Copyright (c) 2005-2010, York Liu <sadly@phpx.com>, Alvaro Videla <videlalvaro@gmail.com>
  All rights reserved.
  
  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are
  met:
  
      * Redistributions of source code must retain the above copyright
        notice, this list of conditions and the following disclaimer.
      * Redistributions in binary form must reproduce the above
        copyright notice, this list of conditions and the following
        disclaimer in the documentation and/or other materials provided
        with the distribution.
      * The names of the contributors may not be used to endorse or promote products
        derived from this software without specific prior written
        permission.
  
  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
/*
 * MyPEB port for PHP 7/8
 * Tested with PHP 7.2.34
 * Kuznetsov O., 2021
 */

/* $Id: header,v 1.16.2.1.2.1 2007/01/01 19:32:09 iliaa Exp $ */

#include "zend_smart_str.h"
#include "php_peb.h"

/****************************************
  macros define
****************************************/

ZEND_DECLARE_MODULE_GLOBALS(peb)

/* True global resources - no need for thread safety here */
static int  le_link, le_plink, le_msgbuff, le_serverpid;
static int  fd;

typedef struct _peb_link {
    ei_cnode*       ec;
    char*           node;
    char*           secret;
    int             fd;
    int             is_persistent;
} peb_link;

/*
 * Every user visible function must have an entry in peb_functions[].
 */
static const zend_function_entry peb_functions[] = {
  PHP_FE(peb_connect, NULL)
  PHP_FE(peb_pconnect, NULL)
  PHP_FE(peb_close, NULL)
  PHP_FE(peb_send_byname, NULL)
  PHP_FE(peb_send_bypid, NULL)
  PHP_FE(peb_rpc, NULL) 
  PHP_FE(peb_rpc_to, NULL)
  PHP_FE(peb_receive, NULL)
  PHP_FE(peb_vencode, NULL)
  PHP_FE(peb_encode, NULL)
  PHP_FE(peb_decode, NULL)
  PHP_FE(peb_vdecode, NULL)
  PHP_FE(peb_error, NULL)
  PHP_FE(peb_errorno, NULL)
  PHP_FE(peb_linkinfo, NULL)
  PHP_FE(peb_status, NULL)
  PHP_FE(peb_print_term, NULL)
  PHP_FE_END
};

/*
 * peb_module_entry
 */
zend_module_entry peb_module_entry = {
#if ZEND_MODULE_API_NO >= 20010901
  STANDARD_MODULE_HEADER,
#endif
  PHP_PEB_EXTNAME,
  peb_functions,
  PHP_MINIT(peb),
  PHP_MSHUTDOWN(peb),
  PHP_RINIT(peb),   /* Replace with NULL if there's nothing to do at request start */
  PHP_RSHUTDOWN(peb), /* Replace with NULL if there's nothing to do at request end */
  PHP_MINFO(peb),
#if ZEND_MODULE_API_NO >= 20010901
  PHP_PEB_VERSION, /* Replace with version number for your extension */
#endif
  STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_PEB
ZEND_GET_MODULE(peb)
#endif

/*
 * PHP_INI
 */
/*PHP_INI_BEGIN()
    STD_PHP_INI_ENTRY("peb.default_nodename", "server@localhost", PHP_INI_ALL, NULL)
    STD_PHP_INI_ENTRY("peb.default_cookie", "COOKIE", PHP_INI_ALL, NULL)
    STD_PHP_INI_ENTRY("peb.default_timeout", "5000", PHP_INI_ALL, NULL)
PHP_INI_END()*/

/*
 * PHP_MINIT_FUNCTION
 */
static ZEND_RSRC_DTOR_FUNC(le_msgbuff_dtor)
{
    if ( res->ptr ) {
        ei_x_buff*      tmp = (ei_x_buff *) res->ptr;

        ei_x_free(tmp);
        efree(tmp);
        res->ptr = NULL;
    }
}

static ZEND_RSRC_DTOR_FUNC(le_serverpid_dtor)
{
    if ( res->ptr ) {
        erlang_pid*     tmp = res->ptr;

        efree(tmp);
        res->ptr = NULL;
    }
}

static ZEND_RSRC_DTOR_FUNC(le_link_dtor)
{
    if ( res->ptr ) {
        peb_link*   tmp = (peb_link *) res->ptr;
        int         p = tmp->is_persistent;

        pefree(tmp->ec, p);
        efree(tmp->node);
        efree(tmp->secret);
#if DEBUG_PRINTF
        php_printf("ZEND_RSRC_DTOR_FUNC called\r\n");
#endif /* DEBUG_PRINTF */

        close(tmp->fd);
        pefree(tmp, p);

        if ( p ) {
            PEB_G(num_persistent)--;
        }
        else {
            PEB_G(num_link)--;
        }

        res->ptr = NULL;
    }
}

/*
 * Module initialisation function
 */
PHP_MINIT_FUNCTION(peb)
{
    PEB_G(default_link) = NULL;
    PEB_G(num_link) = 0;
    PEB_G(num_persistent) = 0;
    PEB_G(error) = NULL;
    PEB_G(errorno) = 0;

    PEB_G(instanceid) = 0;

    le_link = zend_register_list_destructors_ex(le_link_dtor,NULL,PEB_RESOURCENAME,module_number);
    le_plink = zend_register_list_destructors_ex(NULL,le_link_dtor,PEB_RESOURCENAME,module_number);

    le_msgbuff = zend_register_list_destructors_ex(le_msgbuff_dtor,NULL,PEB_TERMRESOURCE,module_number);
    le_serverpid = zend_register_list_destructors_ex(le_serverpid_dtor,NULL,PEB_SERVERPID,module_number);
        
    /*REGISTER_INI_ENTRIES();*/
    return SUCCESS;
}

/*
 * Module shutdown function
 */
PHP_MSHUTDOWN_FUNCTION(peb)
{
    /*UNREGISTER_INI_ENTRIES();*/

    /* release all link resource here */
    if ( PEB_G(error) != NULL ) {
        efree(PEB_G(error));
    }

    return SUCCESS;
}

/*
 * Request initialisation function
 */
PHP_RINIT_FUNCTION(peb)
{
    PEB_G(default_link) = NULL;
    PEB_G(num_link) = PEB_G(num_persistent);
    PEB_G(error) = NULL;
    PEB_G(errorno) = 0;

    return SUCCESS;
}

/*
 * Request shutdown function
 */
PHP_RSHUTDOWN_FUNCTION(peb)
{
    if ( PEB_G(error) != NULL ) {
        efree(PEB_G(error));
    }

    return SUCCESS;
}

/*
 * Module information function
 */
PHP_MINFO_FUNCTION(peb)
{
    php_info_print_table_start();
    php_info_print_table_row(2, "PEB (Php-Erlang Bridge) support", "enabled");
    php_info_print_table_row(2, "version", PHP_PEB_VERSION);
    php_info_print_table_end();

    /*DISPLAY_INI_ENTRIES();*/
}

/*
 * Function outputs extension status information
 *
 * Prototype:
 *      void peb_status()
 *
 * Parameters:
 *      None
 *
 * Return:
 *      None
 */
PHP_FUNCTION(peb_status)
{
    php_printf("\r\n<br>default link: %d", (int) PEB_G(default_link));
    php_printf("\r\n<br>num link: %d", (int) PEB_G(num_link));
    php_printf("\r\n<br>num persistent: %d", (int) PEB_G(num_persistent));
}

/*
 * Function returns an array that contain detailed information about the link
 *
 * Prototype:
 *      array peb_linkinfo([resource linkid])
 *
 * Parameters:
 *      linkid      the erlang node connaction (if the link identifier is not
 *                  specified, the last link opened by peb_connect() is used)
 *
 * Return:
 *      array       assoc array:
 *                      'thishostname' => hostname,
 *                      'thisnodenmae' => nodename,
 *                      'thisalivenmae' => alivename,
 *                      'connectcookie' => cookie,
 *                      'creation' => creation,
 *                      'is_persistent' => is_persistent
 *      false       failure
 */
PHP_FUNCTION(peb_linkinfo)
{
    zend_resource*  linkid;
    zval*           peb_linkid = NULL;
    peb_link*       peb;

    if ( zend_parse_parameters(ZEND_NUM_ARGS(), "|r!", &peb_linkid) == FAILURE ) {
        RETURN_FALSE;
    }

    if ( peb_linkid )  {
        linkid = Z_RES_P(peb_linkid);
    }
    else {
        linkid = PEB_G(default_link);
        if ( !linkid )  {
            RETURN_FALSE;
        }
    }

    if ( (peb=(peb_link*)zend_fetch_resource2(linkid, PEB_RESOURCENAME, le_link, le_plink)) == NULL ) {
        RETURN_FALSE;
    }

    array_init(return_value);
    add_assoc_string(return_value, "thishostname", peb->ec->thishostname);
    add_assoc_string(return_value, "thisnodename", peb->ec->thisnodename);
    add_assoc_string(return_value, "thisalivename", peb->ec->thisalivename);
    add_assoc_string(return_value, "connectcookie", peb->ec->ei_connect_cookie);
    add_assoc_long(return_value, "creation", peb->ec->creation);
    add_assoc_long(return_value, "is_persistent", peb->is_persistent);
}

/*
 * Connect to Erlang node
 */
static void php_peb_connect_impl(INTERNAL_FUNCTION_PARAMETERS, int persistent)
{
    char        *node = NULL, *secret = NULL;
    char        *thisnode = NULL/*, *key = NULL*/;
    int         node_len, secret_len, this_len/*, key_len*/;
    int         instance;

    zend_long   tmo = 0;
    smart_str   key = {0};

    peb_link*   alink = NULL;
    ei_cnode*   ec = NULL;

    if ( zend_parse_parameters(ZEND_NUM_ARGS(), "ss|l", &node, &node_len,
            &secret, &secret_len, &tmo) == FAILURE ) {
        RETURN_FALSE;
    }

#if DEBUG_PRINTF
    php_printf("PEB: php_peb_connect_impl(): connecting to erlang node: %s, timeout: %d\n", node, tmo);
#endif /* DEBUG_PRINTF */

    /*key_len = spprintf(&key, 0, "peb_%s_%s", node, secret);*/
    smart_str_appends(&key, "peb_");
    smart_str_appendl(&key, node, node_len);
    smart_str_appendc(&key, '_');
    smart_str_appendl(&key, secret, secret_len);
    smart_str_0(&key);

    if ( persistent ) {
        zend_resource*  le;

        if ( (le = zend_hash_find_ptr(&EG(persistent_list), key.s)) != NULL ) {
            if ( le->type == le_plink ) {
                alink = (peb_link *) le->ptr;


                RETVAL_RES(zend_register_resource(alink, le_plink));
                PEB_G(default_link) = Z_RES_VAL_P(return_value);
                smart_str_free(&key);
#if DEBUG_PRINTF
                php_printf("PEB: php_peb_connect_impl(): found an existing persistent link\r\n");
#endif /* DEBUG_PRINTF */
                return;
            }
            else {
                php_error(E_WARNING, "PEB: php_peb_connect_impl(): Hash key confilict! "
                                    "Given name associate with non-peb resource!\r\n");
                smart_str_free(&key);
                RETURN_FALSE;
            }
        }
    }

    ec = pemalloc(sizeof(ei_cnode), persistent);
    if ( persistent ) {
        instance = 0;
        this_len = spprintf(&thisnode, 0, "peb_client_%d_%d", getpid(), instance);
    }
    else {
        instance = PEB_G(instanceid)++;
        this_len = spprintf(&thisnode, 0, "peb_client_%d", getpid());
    }

    if ( ei_connect_init(ec, thisnode, secret, instance) < 0 ) {
#if DEBUG_PRINTF
        php_error(E_WARNING, "PEB: php_peb_connect_impl(): connect init failure\r\n");
#endif /* DEBUG_PRINTF */
        PEB_G(errorno) = PEB_ERRORNO_INIT;
        PEB_G(error) = estrdup(PEB_ERROR_INIT);
        efree(thisnode);
        pefree(ec, persistent);
        smart_str_free(&key);
        RETURN_FALSE;
    }

    efree(thisnode);

    if ( (fd = ei_connect_tmo(ec, node, tmo)) < 0 ) {
#if DEBUG_PRINTF
        php_error(E_WARNING, "PEB: php_peb_connect_impl(): connect error :%d\r\n", fd);
#endif /* DEBUG_PRINTF */
        PEB_G(errorno) = PEB_ERRORNO_CONN;
        PEB_G(error) = estrdup(PEB_ERROR_CONN);
        pefree(ec, persistent);
        smart_str_free(&key);
        RETURN_FALSE;
    }

    alink = pemalloc(sizeof(peb_link), persistent);
    alink->ec = ec;
    alink->node = estrndup(node, node_len);
    alink->secret = estrndup(secret, secret_len);
    alink->fd = fd;
    alink->is_persistent = persistent;

    if ( persistent ) {
        zend_resource   newle;

        PEB_G(num_link)++;
        PEB_G(num_persistent)++;

        newle.ptr = alink;
        newle.type = le_plink;

        zend_hash_update_mem(&EG(persistent_list), key.s, (void*)&newle, sizeof(zend_resource));
        /* TODO: check result code */

        RETVAL_RES(zend_register_resource(alink, le_plink));
        PEB_G(default_link) = Z_RES_VAL_P(return_value);
    }
    else {
        PEB_G(num_link)++;
        RETVAL_RES(zend_register_resource(alink, le_link));
    }

    smart_str_free(&key);

#if DEBUG_PRINTF
    php_printf("PEB: php_peb_connect_impl: node %s, connected\r\n", node);
#endif /* DEBUG_PRINTF */
}

/*
 * Open a connection to an Erlang node
 *
 * Prototype:
 *      linkid peb_connect(string nodename, string cookie [, int timeout])
 *
 * Parameters:
 *      nodename    erlang node (dns/ip)
 *      cookie      secret cookie for connecion
 *      timeout     connect timeout in milliseconds, default is no timeout
 *
 * Return:
 *      linkid      link ident, false on error
 */
PHP_FUNCTION(peb_connect)
{
    PEB_G(error) = NULL;
    PEB_G(errorno) = 0;

    php_peb_connect_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, 0);
}

/*
 * Open a permanent connection to an Erlang node
 *
 * Prototype:
 *      linkid peb_connect(string nodename, string cookie [, int timeout])
 *
 * Parameters:
 *      nodename    erlang node (dns/ip)
 *      cookie      secret cookie for connecion
 *      timeout     connect timeout in milliseconds, default is no timeout
 *
 * Return:
 *      linkid      link ident, false on error
 */
PHP_FUNCTION(peb_pconnect)
{
    PEB_G(error) = NULL;
    PEB_G(errorno) = 0;

    php_peb_connect_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, 1);
}

/*
 * Function closes the non-persistent connection to the Erlang node
 * that's associated with the specified link identifier
 *
 * Prototype:
 *      boolean peb_close([resource linkid])
 *
 * Parameters:
 *      linkid      the erlang node connaction (if the link identifier is not
 *                  specified, the last link opened by peb_connect() is used)
 *
 * Return:
 *      true        success
 *      false       close failed
 */
PHP_FUNCTION(peb_close)
{
    zend_resource*  linkid;
    zval*           peb_linkid = NULL;
    peb_link*       peb;

    if ( zend_parse_parameters(ZEND_NUM_ARGS(), "|r!", &peb_linkid) == FAILURE ) {
        RETURN_FALSE;
    }

    if ( !peb_linkid )  {
#if DEBUG_PRINTF
    php_printf("PEB: peb_close(): closing default link\r\n");
#endif /* DEBUG_PRINTF */

        linkid = PEB_G(default_link);
        if ( !linkid )  {
            RETURN_FALSE;
        }

        zend_list_delete(linkid);
        PEB_G(default_link) = NULL;
        RETURN_TRUE;
    }

    linkid = Z_RES_P(peb_linkid);
    if ( zend_fetch_resource2(linkid, PEB_RESOURCENAME, le_link, le_plink) == NULL )  {
#if DEBUG_PRINTF
        php_error(E_WARNING, "PEB: peb_close(): invalid given link\r\n");
#endif /* DEBUG_PRINTF */
        RETURN_FALSE;
    }

    if ( linkid == PEB_G(default_link) ) {
        zend_list_delete(linkid);
        PEB_G(default_link) = NULL;
    }

    zend_list_close(linkid);

#if DEBUG_PRINTF
    php_printf("PEB: peb_close(): link closed\r\n");
#endif /* DEBUG_PRINTF */

    RETURN_TRUE;
}

/*
 * Sends an Erlang message to the Erlang node that's associated
 * with the specified link identifier
 *
 * Prototype:
 *      boolean peb_send_byname(string process_name, resource messageid [, resource linkid [, int timeout]])
 *
 * Parameters:
 *      process_name    registered erlang process name
 *      messageid       formatted message id
 *      linkid          node link identifier (If linkid isn't specified,
 *                      the last opened link is used)
 *      timeout         send timeout in milliseconds, default is no timeout
 *
 * Return:
 *      true            send successfull
 *      false           send failure
 */
PHP_FUNCTION(peb_send_byname)
{
    char*           process_name;
    int             process_len, result;
    zval*           message = NULL;
    zend_resource*  linkid;
    zval*           peb_linkid = NULL;
    peb_link*       peb;
    zend_long       tmo = 0;
    ei_x_buff*      newbuff;

    PEB_G(error) = NULL;
    PEB_G(errorno) = 0;

    if ( zend_parse_parameters(ZEND_NUM_ARGS(), "sr|rl", &process_name, &process_len,
                &message, &peb_linkid, &tmo) == FAILURE ) {
        RETURN_FALSE;
    }

    if ( ZEND_NUM_ARGS() > 2 )  {
        linkid = Z_RES_P(peb_linkid);
    }
    else {
        linkid = PEB_G(default_link);
        if ( !linkid )  {
            RETURN_FALSE;
        }
    }

    if ( (peb=(peb_link*)zend_fetch_resource2(linkid, PEB_RESOURCENAME, le_link, le_plink)) == NULL )  {
        RETURN_FALSE;
    }

    if ( (newbuff=(ei_x_buff*)zend_fetch_resource(Z_RES_P(message), PEB_TERMRESOURCE, le_msgbuff)) == NULL ) {
        RETURN_FALSE;
    }

#if DEBUG_PRINTF
    php_printf("PEB: peb_send_byname(): [link: fd=%d, node='%s']: process: '%s', buff: '%s', timeout %d msecs\r\n",
                    peb->fd, peb->node, process_name, newbuff->buff, tmo);
#endif /* DEBUG_PRINTF */

    result = ei_reg_send_tmo(peb->ec, peb->fd, process_name, newbuff->buff, newbuff->index, tmo);

    if ( result < 0 ) {
        /* process peb_error here */
#if DEBUG_PRINTF
        php_error(E_WARNING, "PEB: peb_send_byname(): failed, result: %d\r\n", result);
#endif /* DEBUG_PRINTF */

        PEB_G(errorno) = PEB_ERRORNO_SEND;
        PEB_G(error) = estrdup(PEB_ERROR_SEND);
        RETURN_FALSE;
    }

    RETURN_TRUE;
}

/*
 * Sends an Erlang message to the Erlang node that's associated
 * with the specified link identifier
 *
 * Prototype:
 *      boolean peb_send_byname(resource process_id, resource messageid [, resource linkid [, int timeout]])
 *
 * Parameters:
 *      process_id      process identifier
 *      messageid       formatted message id
 *      linkid          node link identifier (If linkid isn't specified,
 *                      the last opened link is used)
 *      timeout         send timeout in milliseconds, default is no timeout
 *
 * Return:
 *      true            send successfull
 *      false           send failure
 */
PHP_FUNCTION(peb_send_bypid)
{
    zend_resource*  linkid;
    zval*           peb_linkid = NULL;
    peb_link*       peb;
    zval*           pid = NULL;
    zval*           message = NULL;
    zend_long       tmo = 0;
    erlang_pid*     serverpid;
    ei_x_buff*      newbuff;
    int             result;

    PEB_G(error) = NULL;
    PEB_G(errorno) = 0;

    if ( zend_parse_parameters(ZEND_NUM_ARGS(), "rr|rl", &pid, &message,
            &peb_linkid, &tmo) == FAILURE ) {
        RETURN_FALSE;
    }

    if ( ZEND_NUM_ARGS() > 2 )  {
        linkid = Z_RES_P(peb_linkid);
    }
    else {
        linkid = PEB_G(default_link);
        if ( !linkid )  {
            RETURN_FALSE;
        }
    }

    if ( (peb=(peb_link*)zend_fetch_resource2(linkid, PEB_RESOURCENAME, le_link, le_plink)) == NULL )  {
        RETURN_FALSE;
    }

    if ( (newbuff=(ei_x_buff*)zend_fetch_resource(Z_RES_P(message), PEB_TERMRESOURCE, le_msgbuff)) == NULL ) {
        RETURN_FALSE;
    }

    if ( (serverpid=(erlang_pid*)zend_fetch_resource(Z_RES_P(pid), PEB_SERVERPID, le_serverpid)) == NULL ) {
        RETURN_FALSE;
    }

    result = ei_send_tmo(peb->fd, serverpid, newbuff->buff, newbuff->index, tmo);
    if ( result < 0 ) {
        /* process peb_error here */
        PEB_G(errorno) = PEB_ERRORNO_SEND;
        PEB_G(error) = estrdup(PEB_ERROR_SEND);
        RETURN_FALSE;
    }

    RETURN_TRUE;
}

/*
 * Receive a message from the Erlang node that's associated
 * with the specified link identifier
 *
 * Prototype:
 *      resource peb_receive([resource linkid [, int timeout]])
 *
 * Parameters:
 *      linkid          node link identifier (If linkid isn't specified,
 *                      the last opened link is used)
 *      timeout         send timeout in milliseconds, default is no timeout
 *
 * Return:
 *      messageid       message received
 *      false           receive failed
 */
PHP_FUNCTION(peb_receive)
{
    zend_resource*  linkid;
    zval*           peb_linkid = NULL;
    peb_link*       peb;
    zend_long       tmo = 0;
    ei_x_buff*      newbuff;
    erlang_msg      message;
    int             result;

    PEB_G(error) = NULL;
    PEB_G(errorno) = 0;

    if ( zend_parse_parameters(ZEND_NUM_ARGS(), "|rl", &peb_linkid, &tmo) == FAILURE ) {
        RETURN_FALSE;
    }

    if ( ZEND_NUM_ARGS() > 0 )  {
        linkid = Z_RES_P(peb_linkid);
    }
    else {
        linkid = PEB_G(default_link);
        if ( !linkid )  {
            RETURN_FALSE;
        }
    }

    if ( (peb=(peb_link*)zend_fetch_resource2(linkid, PEB_RESOURCENAME, le_link, le_plink)) == NULL )  {
        RETURN_FALSE;
    }

    newbuff = emalloc(sizeof(ei_x_buff));
    ei_x_new(newbuff);

    while ( 1 ) {
        result = ei_xreceive_msg_tmo(peb->fd, &message, newbuff, tmo);

        switch ( result ) {
            case ERL_TICK: /* idle */
                break;

            case ERL_MSG:
                if ( message.msgtype == ERL_SEND ) {
                    RETVAL_RES(zend_register_resource(newbuff, le_msgbuff));
                    return;
                }
                else {
                    /* php_printf("error: not erl_send\r\n"); */
                    PEB_G(errorno) = PEB_ERRORNO_NOTMINE;
                    PEB_G(error) = estrdup(PEB_ERROR_NOTMINE);
                    ei_x_free(newbuff);
                    efree(newbuff);
                    RETURN_FALSE;
                }
                break;

            default:
                /* php_printf("error: unknown ret %d\r\n",ret); */
                PEB_G(errorno) = PEB_ERRORNO_RECV;
                PEB_G(error) = estrdup(PEB_ERROR_RECV);
                ei_x_free(newbuff);
                efree(newbuff);
                RETURN_FALSE;
        }
    }

    RETURN_TRUE;
}

/*
 * Functiomn sends and receive an RPC request to/from a remote node
 *
 * Prototype:
 *      resource peb_rpc(string module, string function, resource message [, resource link_identifier])
 *
 * Parameters:
 *      module          module name
 *      function        function name
 *      messageid       formatted message id
 *      linkid          node link identifier (If linkid isn't specified,
 *                      the last opened link is used)
 * Return:
 *      messageid       message received
 *      false           receive failed
 */
PHP_FUNCTION(peb_rpc)
{
    zend_resource*  linkid;
    zval*           peb_linkid = NULL;
    peb_link*       peb;
    zval*           message = NULL;
    char            *module, *func;
    int             module_len, func_len;
    ei_x_buff*      newbuff;
    ei_x_buff*      result_buff;
    int             result;

    PEB_G(error) = NULL;
    PEB_G(errorno) = 0;

    if ( zend_parse_parameters(ZEND_NUM_ARGS(), "ssr|r!", &module, &module_len,
            &func, &func_len, &message, &peb_linkid) == FAILURE )  {
        RETURN_FALSE;
    }

    if ( peb_linkid )  {
        linkid = Z_RES_P(peb_linkid);
    }
    else {
        linkid = PEB_G(default_link);
        if ( !linkid )  {
            RETURN_FALSE;
        }
    }

    if ( (peb=(peb_link*)zend_fetch_resource2(linkid, PEB_RESOURCENAME, le_link, le_plink)) == NULL )  {
        RETURN_FALSE;
    }

    if ( (newbuff=(ei_x_buff*)zend_fetch_resource(Z_RES_P(message), PEB_TERMRESOURCE, le_msgbuff)) == NULL ) {
        RETURN_FALSE;
    }

    result_buff = emalloc(sizeof(ei_x_buff));
    ei_x_new(result_buff);

    result = ei_rpc(peb->ec, peb->fd, module, func, newbuff->buff, newbuff->index, result_buff);

    //php_printf("ei_rpc ret: %d\r\n<br />", result);

    if ( result < 0 ) {
        /* process peb_error here */
        PEB_G(errorno) = PEB_ERRORNO_SEND;
        PEB_G(error) = estrdup(PEB_ERROR_SEND);

        ei_x_free(result_buff);
        efree(result_buff);
        RETURN_FALSE;
    }

    RETVAL_RES(zend_register_resource(result_buff, le_msgbuff));
}

/*
 * Functiomn sends an RPC request to a remote node
 *
 * Prototype:
 *      boolean peb_rpc_to(string module, string function, resource message [, resource link_identifier])
 *
 * Parameters:
 *      module          module name
 *      function        function name
 *      messageid       formatted message id
 *      linkid          node link identifier (If linkid isn't specified,
 *                      the last opened link is used)
 * Return:
 *     true             rpc send successfully
 *     false            rpc failure
 */
PHP_FUNCTION(peb_rpc_to)
{
    zend_resource*  linkid;
    zval*           peb_linkid = NULL;
    peb_link*       peb;
    zval*           message = NULL;
    char            *module, *func;
    int             module_len, func_len;
    ei_x_buff*      newbuff;
    int             result;

    PEB_G(error) = NULL;
    PEB_G(errorno) = 0;

    if ( zend_parse_parameters(ZEND_NUM_ARGS(), "ssr|r!", &module, &module_len,
            &func, &func_len, &message, &peb_linkid) == FAILURE )  {
        RETURN_FALSE;
    }

    if ( peb_linkid )  {
        linkid = Z_RES_P(peb_linkid);
    }
    else {
        linkid = PEB_G(default_link);
        if ( !linkid )  {
            RETURN_FALSE;
        }
    }

    if ( (peb=(peb_link*)zend_fetch_resource2(linkid, PEB_RESOURCENAME, le_link, le_plink)) == NULL )  {
        RETURN_FALSE;
    }

    if ( (newbuff=(ei_x_buff*)zend_fetch_resource(Z_RES_P(message), PEB_TERMRESOURCE, le_msgbuff)) == NULL ) {
        RETURN_FALSE;
    }

    result = ei_rpc_to(peb->ec, peb->fd, module, func, newbuff->buff, newbuff->index);
    if ( result < 0 ) {
        /* process peb_error here */
        PEB_G(errorno) = PEB_ERRORNO_SEND;
        PEB_G(error) = estrdup(PEB_ERROR_SEND);
        RETURN_FALSE;
    }

    RETURN_TRUE;
}

static void _peb_encode_term(ei_x_buff* x, char** fmt, int* fmtpos, HashTable* arr, zend_long* arridx)
{
    char*           p = *fmt + *fmtpos;
    int             i,v;
    zval*           pdata;
    ei_x_buff       *newbuff, decoded;
    peb_link*       peb;
    erlang_pid*     ep;

    ++p;
    (*fmtpos)++;

    while ( *p == ' ' ) {
        ++p;
        (*fmtpos)++;
    }

    switch ( *p ) {
        case 'a':
            if ( (pdata=zend_hash_index_find(arr, (zend_long)(*arridx))) != NULL ) {
                newbuff = emalloc(sizeof(ei_x_buff));
                ei_x_new(newbuff);
                ei_x_encode_atom(newbuff, Z_STRVAL_P(pdata));
                ei_x_append(x, newbuff);
                ei_x_free(newbuff);
                efree(newbuff);
            }

            ++(*arridx);
            break;

        case 's':
            if ( (pdata=zend_hash_index_find(arr, *arridx)) != NULL ) {
                newbuff = emalloc(sizeof(ei_x_buff));
                ei_x_new(newbuff);
                ei_x_encode_string_len(newbuff, Z_STRVAL_P(pdata), Z_STRLEN_P(pdata));
                ei_x_append(x, newbuff);
                ei_x_free(newbuff);
                efree(newbuff);
            }
            ++(*arridx);
            break;

        case 'b':
            if ( (pdata=zend_hash_index_find(arr, *arridx)) != NULL ) {
                newbuff = emalloc(sizeof(ei_x_buff));
                ei_x_new(newbuff);
                ei_x_encode_binary(newbuff, Z_STRVAL_P(pdata),Z_STRLEN_P(pdata));
                ei_x_append(x, newbuff);
                ei_x_free(newbuff);
                efree(newbuff);
            }
            ++(*arridx);
            break;

        case 'i':
        case 'l':
        case 'u':
            if( (pdata=zend_hash_index_find(arr, *arridx)) != NULL ) {
                newbuff = emalloc(sizeof(ei_x_buff));
                ei_x_new(newbuff);
                ei_x_encode_long(newbuff, Z_LVAL_P(pdata));
                ei_x_append(x, newbuff);
                ei_x_free(newbuff);
                efree(newbuff);
            }
            ++(*arridx);
            break;

        case 'f':
        case 'd':
            if ( (pdata=zend_hash_index_find(arr, *arridx)) != NULL ) {
                newbuff = emalloc(sizeof(ei_x_buff));
                ei_x_new(newbuff);
                ei_x_encode_double(newbuff, Z_DVAL_P(pdata));
                ei_x_append(x, newbuff);
                ei_x_free(newbuff);
                efree(newbuff);
            }
            ++(*arridx);
            break;

        case 'p':
            if ( (pdata=zend_hash_index_find(arr, *arridx)) != NULL ) {
                //m = (peb_link*) zend_fetch_resource(pdata TSRMLS_CC,-1 , PEB_RESOURCENAME , NULL, 2, le_link, le_plink);
                peb = (peb_link*)zend_fetch_resource2_ex(pdata, PEB_RESOURCENAME, le_link, le_plink);
                if ( peb ) {
                    newbuff = emalloc(sizeof(ei_x_buff));
                    ei_x_new(newbuff);
                    ei_x_encode_pid(newbuff, &(peb->ec->self));
                    ei_x_append(x, newbuff);
                    ei_x_free(newbuff);
                    efree(newbuff);
                }
            }
            ++(*arridx);
            break;

        case 'P':
            if ( (pdata=zend_hash_index_find(arr, *arridx)) != NULL ) {
                //ep = (erlang_pid*) zend_fetch_resource(pdata TSRMLS_CC,-1 , PEB_SERVERPID , NULL, 1, le_serverpid);
                ep = (erlang_pid*)zend_fetch_resource_ex(pdata, PEB_SERVERPID, le_serverpid);
                if ( ep ) {
                    newbuff = emalloc(sizeof(ei_x_buff));
                    ei_x_new(newbuff);
                    ei_x_encode_pid(newbuff, ep);
                    ei_x_append(x, newbuff);
                    ei_x_free(newbuff);
                    efree(newbuff);
                }
            }
            ++(*arridx);
            break;

        case ',':
        case '~':
            break;

        default:
            return;
    }

    _peb_encode_term(x, fmt, fmtpos, arr, arridx);
}

/*
 *  ~a - an atom, char*
 *  ~s - a string, char*
 *  ~b - a binary, char*
 *  ~i - an integer, int
 *  ~l - a long integer, long int
 *  ~u - an unsigned long integer, unsigned long int
 *  ~f - a float, float
 *  ~d - a double float, double float
 *  ~p - an erlang pid
 */
static void _peb_encode(ei_x_buff* x, char** fmt, int fmt_len, int* fmtpos, HashTable* arr, zend_long* arridx)
{
    char*           p = *fmt + (*fmtpos);
    int             res;
    zend_long       newidx = 0;

    HashTable*      newarr;
    zval*           tmp;
    ei_x_buff*      newbuff;

    while ( *p == ' ' ) {
        ++p;
        (*fmtpos)++;
    }

    // Special case, empty list.
    if ( *p == '[' && *(p+1) == ']') {
        // php_printf("Inside IF: fmt_len: %d\n", fmt_len);

        ei_x_encode_empty_list(x);
        ++p; //consume current char
        (*fmtpos)++;
        ++p; //consume ] char
        (*fmtpos)++;
  
        (*arridx)++;
  
        if ( (fmt_len-1) <= *fmtpos ) {
            // php_printf("\n\n\n\nfmt_len: %d fmtpos %d\n\n\n\n", fmt_len, *fmtpos);
            return;
        }
        _peb_encode(x,fmt,fmt_len,fmtpos,arr,arridx);
    }

    switch ( *p ) {
        case '~':
            _peb_encode_term(x, fmt, fmtpos, arr, arridx);
            break;

        case '[':
            if ( (tmp=zend_hash_index_find(arr, *arridx)) != NULL) {
                ZVAL_DEREF(tmp);
                newarr = Z_ARRVAL_P(tmp);
        
                //empty list handling
                if ( zend_hash_num_elements(newarr) == 0 && *(p+1) == '[' && *(p+2) == ']') {
                    ei_x_encode_empty_list(x);
                    ++p;                //advance from current char
                    (*fmtpos)++;
                    ++p;                //skip [
                    (*fmtpos)++;
                    ++p;                //skip ]
                    (*fmtpos)++;
                    (*arridx)++;
                    break;
                }
          
                ++p;
                (*fmtpos)++;
                newbuff = emalloc(sizeof(ei_x_buff));
                ei_x_new(newbuff);

                _peb_encode(newbuff, fmt, fmt_len, fmtpos, newarr, &newidx);
       
                if( newidx != 0 ) {
                    /* php_printf("newidx:%d",newidx); */
                    ei_x_encode_list_header(x, newidx);
                    ei_x_append(x, newbuff);
                    ei_x_encode_empty_list(x);
                    ei_x_free(newbuff);
                    efree(newbuff);
                }
                else {
                    ei_x_free(newbuff);
                    efree(newbuff);
                }
            }
            (*arridx)++;
            break;

        case ']':
            ++p;
            (*fmtpos)++;
            return;
            break;

        case '{':
            if ( (tmp=zend_hash_index_find(arr, *arridx)) != NULL) {
                ZVAL_DEREF(tmp);
                newarr = Z_ARRVAL_P(tmp);

                ++p;
                (*fmtpos)++;
                newbuff = emalloc(sizeof(ei_x_buff));
                ei_x_new(newbuff);

                _peb_encode(newbuff, fmt, fmt_len, fmtpos, newarr, &newidx);
                if ( newidx !=0 ) {
                    /* php_printf("newidx:%d",newidx); */
                    ei_x_encode_tuple_header(x, newidx);
                    ei_x_append(x, newbuff);
                    ei_x_free(newbuff);
                    efree(newbuff);
                }
                else {
                    ei_x_free(newbuff);
                    efree(newbuff);
                }
            }
            (*arridx)++;
            break;

        case '}':
            ++p;
            (*fmtpos)++;
            return;
            break;

        case ',':
            ++p;
            (*fmtpos)++;
            break;

        default:
            return;
            break;
    }

    _peb_encode(x, fmt, fmt_len, fmtpos, arr, arridx);
}

static void php_peb_encode_impl(INTERNAL_FUNCTION_PARAMETERS, int with_version)
{
    char*           fmt;
    int             fmt_len;
    int             fmtpos = 0;
    int             res;
    zend_long       arridx = 0;

    zval*           tmp;
    ei_x_buff*      x;
    HashTable*      htable;

    if ( zend_parse_parameters(ZEND_NUM_ARGS(), "sa", &fmt, &fmt_len, &tmp) == FAILURE )  {
        RETURN_FALSE;
    }

    /* find hashtable for array */
    ZVAL_DEREF(tmp);
    htable = Z_ARRVAL_P(tmp);

    x = emalloc(sizeof(ei_x_buff));
    if ( with_version ) {
        ei_x_new_with_version(x);
    }
    else {
        ei_x_new(x);
    }

    _peb_encode(x, &fmt, fmt_len, &fmtpos, htable, &arridx);
    RETVAL_RES(zend_register_resource(x, le_msgbuff));
}

/*
 * Encodes the PHP variables to Erlang terms according to the specified format
 * with version number
 *
 * Prototype:
 *      resource peb_vencode(string format, array data)
 *
 * Parameters:
 *      format          format string
 *      data            array data to formatting
 *
 * Return:
 *     messageid        success
 *     false            failure
 */
PHP_FUNCTION(peb_vencode)
{
    PEB_G(error) = NULL;
    PEB_G(errorno) = 0;

    php_peb_encode_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, 1);
}

/*
 * Encodes the PHP variables to Erlang terms according to the specified format
 * without version number
 *
 * Prototype:
 *      resource peb_encode(string format, array data)
 *
 * Parameters:
 *      format          format string
 *      data            array data to formatting
 *
 * Return:
 *     messageid        success
 *     false            failure
 */
PHP_FUNCTION(peb_encode)
{
    PEB_G(error) = NULL;
    PEB_G(errorno) = 0;

    php_peb_encode_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, 0);
}

static int _peb_decode(ei_x_buff* x, zval* htable) {
    zval        z;
    int         type;
    int         size;
    char*       buff;
    long        len;
    long        long_value;
    double      double_value;
    int         i;
  
    ei_get_type(x->buff, &x->index, &type, &size);

    switch ( type )  {
        case ERL_ATOM_EXT:
            buff = emalloc(size+1);
            ei_decode_atom(x->buff, &x->index, buff);
            buff[size] = '\0';
            ZVAL_STRING(&z, buff);
            add_next_index_zval(htable, &z);
            efree(buff);
            break;

        case ERL_STRING_EXT:
            buff = emalloc(size+1);
            ei_decode_string(x->buff, &x->index, buff);
            buff[size] = '\0';
            ZVAL_STRING(&z, buff);
            add_next_index_zval(htable, &z);
            efree(buff);
            break;

        case ERL_BINARY_EXT:
            buff = emalloc(size);
            ei_decode_binary(x->buff, &x->index, buff, &len);
            ZVAL_STRINGL(&z, buff, size);
            add_next_index_zval(htable, &z);
            efree(buff);
            break;

        case ERL_PID_EXT:
            buff = emalloc(sizeof(erlang_pid));
            ei_decode_pid(x->buff, &x->index, (erlang_pid*)buff);
            //ZEND_REGISTER_RESOURCE(z, buff, le_serverpid);
            ZVAL_RES(&z, zend_register_resource(buff, le_serverpid));
            add_next_index_zval(htable, &z);
            break;

        case ERL_SMALL_BIG_EXT:
        case ERL_SMALL_INTEGER_EXT:
        case ERL_INTEGER_EXT:
            ei_decode_long(x->buff, &x->index, &long_value);
            ZVAL_LONG(&z, long_value);
            add_next_index_zval(htable, &z);
            break;

        case ERL_FLOAT_EXT:
            ei_decode_double(x->buff, &x->index, &double_value);
            ZVAL_DOUBLE(&z, double_value);
            add_next_index_zval(htable, &z);
            break;

        case ERL_SMALL_TUPLE_EXT:
        case ERL_LARGE_TUPLE_EXT:
            array_init(&z);
            ei_decode_tuple_header(x->buff, &x->index, &size);

            for(i=1; i<=size; i++) {
                if ( _peb_decode(x, &z) != SUCCESS )  {
                    return FAILURE;
                }
            }

            add_next_index_zval(htable, &z);
            break;

        case ERL_NIL_EXT:
        case ERL_LIST_EXT:
            array_init(&z);
            ei_decode_list_header(x->buff, &x->index, &size);

            while ( size > 0 ) {
                for(i=1; i<=size; i++)  {
                    if ( _peb_decode(x, &z) != SUCCESS ) {
                        return FAILURE;
                    }
                }
                ei_decode_list_header(x->buff, &x->index, &size);
            }

            add_next_index_zval(htable, &z);
            break;

        default:
            php_error(E_ERROR, "unsupported data type %d", type);
            PEB_G(errorno) = PEB_ERRORNO_DECODE;
            PEB_G(error) = estrdup(PEB_ERROR_DECODE);
            return FAILURE;
    }

    return SUCCESS;
}

static void php_peb_decode_impl(INTERNAL_FUNCTION_PARAMETERS, int with_version)
{
    zval*       tmp;
    ei_x_buff*  x;
    int         v, result;
    zval        htable;

    if ( zend_parse_parameters(ZEND_NUM_ARGS(), "r", &tmp) == FAILURE )  {
        RETURN_FALSE;
    }

    if ( (x=(ei_x_buff*)zend_fetch_resource(Z_RES_P(tmp), PEB_TERMRESOURCE, le_msgbuff)) == NULL ) {
        RETURN_FALSE;
    }

    x->index = 0;
    if ( with_version ) {
        ei_decode_version(x->buff, &x->index, &v);
    }

    array_init(&htable);

    result = _peb_decode(x, &htable);
    if ( result == SUCCESS ) {
        RETURN_ARR(Z_ARRVAL_P(&htable));
    }
    else {
        RETURN_FALSE;
    }
}

/*
 * Decodes and Erlang term that was send without version magic number
 *
 * Prototype:
 *      mixed peb_decode(resource msgbuffer)
 *
 * Parameters:
 *      msgbuffer       message
 *
 * Return:
 *     array            decodes assoc array
 *     false            decode failure
 */
PHP_FUNCTION(peb_decode)
{
    PEB_G(error) = NULL;
    PEB_G(errorno) = 0;

    php_peb_decode_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, 0);
}

/*
 * Decodes and Erlang term that was send with version magic number
 *
 * Prototype:
 *      mixed peb_vdecode(resource msgbuffer)
 *
 * Parameters:
 *      msgbuffer       message
 *
 * Return:
 *     array            decodes assoc array
 *     false            decode failure
 */
PHP_FUNCTION(peb_vdecode)
{
    PEB_G(error) = NULL;
    PEB_G(errorno) = 0;

    php_peb_decode_impl(INTERNAL_FUNCTION_PARAM_PASSTHRU, 1);
}

/*
 * Get the error message from the last peb function call that produced an error
 *
 * Prototype:
 *      string peb_error()
 *
 * Parameters:
 *      None
 *
 * Return:
 *      error string
 */
PHP_FUNCTION(peb_error)
{
    if ( PEB_G(error) != NULL ) {
        RETURN_STRING(PEB_G(error));
    }
}

/*
 * Get the error number from the last peb function call that produced an error
 *
 * Prototype:
 *      int peb_errorno()
 *
 * Parameters:
 *      None
 *
 * Return:
 *      error code (may be 0)
 */
PHP_FUNCTION(peb_errorno)
{
    RETURN_LONG(PEB_G(errorno));
}

/* {{{ proto resource peb_print_term(resource $term [, bool $return = false])
   Prints the erlang term to the screen
   If $return is set to true, then it returns the string instead of priting it */
PHP_FUNCTION(peb_print_term)
{
    char*       term = NULL;
    zval*       msg = NULL;
    ei_x_buff*  newbuff;
    int         intp = 0;
    int         ret = 0;

    PEB_G(error) = NULL;
    PEB_G(errorno) = 0;

    if ( zend_parse_parameters(ZEND_NUM_ARGS(), "s|b", &msg, &ret) == FAILURE )  {
        RETURN_FALSE;
    }

    if ( (newbuff=(ei_x_buff*)zend_fetch_resource(Z_RES_P(msg), PEB_TERMRESOURCE, le_msgbuff)) == NULL ) {
        RETURN_FALSE;
    }

    ei_s_print_term(&term, newbuff->buff, &intp);
    if ( ret )  {
        RETVAL_STRING(term);
    }
    else {
        php_printf("%s", term);
    }

    free(term);
}