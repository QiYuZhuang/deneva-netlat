/*
   Copyright 2016 Massachusetts Institute of Technology

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

	   http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "global.h"
#include "helper.h"
#include "tpcc.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "tpcc_helper.h"
#include "row.h"
#include "query.h"
#include "txn.h"
#include "mem_alloc.h"
#include "tpcc_const.h"

RC TPCCWorkload::init() {
	Workload::init();
	//char * cpath = getenv("GRAPHITE_HOME");
	char * cpath = getenv("SCHEMA_PATH");
	string path;
	if (cpath == NULL)
		path = "./benchmarks/";
	else {
		path = string(cpath);
		// Graphite:
		//path += "/tests/apps/dbms/";
	}
#if TPCC_SMALL
	path += "TPCC_short_schema.txt";
#else
	path += "TPCC_full_schema.txt";
#endif
	cout << "reading schema file: " << path << endl;
	delivering = new bool * [g_num_wh + 1];
	for (UInt32 wid = 1; wid <= g_num_wh; wid ++)
		delivering[wid] = (bool *) mem_allocator.alloc(CL_SIZE);

  printf("Initializing schema... ");
  fflush(stdout);
	init_schema( path.c_str() );
  printf("Done\n");
  printf("Initializing table... ");
  fflush(stdout);
	init_table();
  printf("Done\n");
  fflush(stdout);
	return RCOK;
}

RC TPCCWorkload::init_schema(const char * schema_file) {
	Workload::init_schema(schema_file);
	t_warehouse = tables["WAREHOUSE"];
	t_district = tables["DISTRICT"];
	t_customer = tables["CUSTOMER"];
	t_history = tables["HISTORY"];
	t_neworder = tables["NEW-ORDER"];
	t_order = tables["ORDER"];
	t_orderline = tables["ORDER-LINE"];
	t_item = tables["ITEM"];
	t_stock = tables["STOCK"];

	i_item = indexes["ITEM_IDX"];
	i_warehouse = indexes["WAREHOUSE_IDX"];
	i_district = indexes["DISTRICT_IDX"];
	i_customer_id = indexes["CUSTOMER_ID_IDX"];
	i_customer_last = indexes["CUSTOMER_LAST_IDX"];
	i_stock = indexes["STOCK_IDX"];
//	i_order = indexes["ORDER_IDX"];
//	i_orderline = indexes["ORDER-LINE_IDX"];
	return RCOK;
}

RC TPCCWorkload::init_table() {
	num_wh = g_num_wh;

/******** fill in data ************/
// data filling process:
//- item
//- wh
//	- stock
// 	- dist
//  	- cust
//	  	- hist
//		- order
//		- new order
//		- order line
/**********************************/

	pthread_t * p_thds = new pthread_t[g_init_parallelism - 1];
  thr_args * tt = new thr_args[g_init_parallelism];
	for (UInt32 i = 0; i < g_init_parallelism ; i++) {
	tt[i].wl = this;
	tt[i].id = i;
  }
  // Stock table
	for (UInt32 i = 0; i < g_init_parallelism - 1; i++) {
	pthread_create(&p_thds[i], NULL, threadInitStock, &tt[i]);
	}
  threadInitStock(&tt[g_init_parallelism-1]);
	for (UInt32 i = 0; i < g_init_parallelism - 1; i++) {
		int rc = pthread_join(p_thds[i], NULL);
		if (rc) {
			printf("ERROR; return code from pthread_join() is %d\n", rc);
			exit(-1);
		}
  }
  printf("STOCK Done\n");
  fflush(stdout);
  // Item Table
	for (UInt32 i = 0; i < g_init_parallelism - 1; i++) {
	pthread_create(&p_thds[i], NULL, threadInitItem, &tt[i]);
	}
  threadInitItem(&tt[g_init_parallelism-1]);
	for (UInt32 i = 0; i < g_init_parallelism - 1; i++) {
		int rc = pthread_join(p_thds[i], NULL);
		if (rc) {
			printf("ERROR; return code from pthread_join() is %d\n", rc);
			exit(-1);
		}
  }
  printf("ITEM Done\n");
  fflush(stdout);
  // Customer Table
	for (UInt32 i = 0; i < g_init_parallelism - 1; i++) {
	pthread_create(&p_thds[i], NULL, threadInitCust, &tt[i]);
	}
  threadInitCust(&tt[g_init_parallelism-1]);
	for (UInt32 i = 0; i < g_init_parallelism - 1; i++) {
		int rc = pthread_join(p_thds[i], NULL);
		if (rc) {
			printf("ERROR; return code from pthread_join() is %d\n", rc);
			exit(-1);
		}
  }
  printf("ITEM Done\n");
  fflush(stdout);

  // Order Table
  /*
	for (UInt32 i = 0; i < g_init_parallelism - 1; i++) {
	pthread_create(&p_thds[i], NULL, threadInitOrder, &tt[i]);
	}
  threadInitOrder(&tt[g_init_parallelism-1]);
	for (UInt32 i = 0; i < g_init_parallelism - 1; i++) {
		int rc = pthread_join(p_thds[i], NULL);
		if (rc) {
			printf("ERROR; return code from pthread_join() is %d\n", rc);
			exit(-1);
		}
  }
  printf("ORDER Done\n");
  fflush(stdout);
  */
	threadInitWh(this);
  printf("WAREHOUSE Done\n");
  fflush(stdout);
	threadInitDist(this);
  printf("DISTRICT Done\n");
  fflush(stdout);
  threadInitHist(this);
  printf("HISTORY Done\n");
  fflush(stdout);

  /*
  UInt32 cust_thr_cnt = g_dist_per_wh/2;
  thr_args * tt = new thr_args[cust_thr_cnt];
	pthread_t * p_thds = new pthread_t[2+cust_thr_cnt];
	pthread_create(&p_thds[0], NULL, threadInitStock, this);
	pthread_create(&p_thds[1], NULL, threadInitHist, this);
  for(UInt32 i=0;i<cust_thr_cnt;i++) {
	tt[i].wl = this;
	tt[i].id = i;
	tt[i].tot = cust_thr_cnt;
	  pthread_create(&p_thds[2+i], NULL, threadInitCust, &tt[i]);
  }
	threadInitOrder(this);
	threadInitItem(this);
	threadInitWh(this);
	threadInitDist(this);

	for (uint32_t i = 0; i < 2 + cust_thr_cnt; i++) {
		int rc = pthread_join(p_thds[i], NULL);
		printf("thread %d complete\n", i);
		if (rc) {
			printf("ERROR; return code from pthread_join() is %d\n", rc);
			exit(-1);
		}
	}
  */
	printf("\nData Initialization Complete!\n\n");
	return RCOK;
}

RC TPCCWorkload::get_txn_man(TxnManager *& txn_manager) {
  DEBUG_M("TPCCWorkload::get_txn_man TPCCTxnManager alloc\n");
  txn_manager = (TPCCTxnManager *)mem_allocator.align_alloc(sizeof(TPCCTxnManager));
	new(txn_manager) TPCCTxnManager();
	//txn_manager->init( this);
	return RCOK;
}

void TPCCWorkload::init_tab_item(int id) {
  if (WL_VERB) printf("[init] loading item table\n");
	for (UInt32 i = id+1; i <= g_max_items; i+=g_init_parallelism) {
		row_t * row;
		uint64_t row_id;
		t_item->get_new_row(row, 0, row_id);
		row->set_primary_key(i);
		row->set_value(I_ID, i);
		row->set_value(I_IM_ID, URand(1L,10000L));
		char name[24];
		//MakeAlphaString(14, 24, name);
		row->set_value(I_NAME, name);
		row->set_value(I_PRICE, URand(1, 100));
		char data[50];
	//MakeAlphaString(26, 50, data);
	if (RAND(10) == 0) strcpy(data, "original");
		row->set_value(I_DATA, data);

		index_insert(i_item, i, row, 0);
	}
}

void TPCCWorkload::init_tab_wh() {
  if (WL_VERB) printf("[init] workload table.\n");
	for (UInt32 wid = 1; wid <= g_num_wh; wid ++) {
	if(GET_NODE_ID(wh_to_part(wid)) != g_node_id) continue;
		row_t * row;
		uint64_t row_id;
		t_warehouse->get_new_row(row, 0, row_id);
		row->set_primary_key(wid);

		row->set_value(W_ID, wid);
		char name[10];
		MakeAlphaString(6, 10, name);
		row->set_value(W_NAME, name);
		char street[20];
		MakeAlphaString(10, 20, street);
		row->set_value(W_STREET_1, street);
		MakeAlphaString(10, 20, street);
		row->set_value(W_STREET_2, street);
		MakeAlphaString(10, 20, street);
		row->set_value(W_CITY, street);
		char state[2];
		MakeAlphaString(2, 2, state); /* State */
		row->set_value(W_STATE, state);
		char zip[9];
		MakeNumberString(9, 9, zip); /* Zip */
		row->set_value(W_ZIP, zip);
		double tax = (double)URand(0L,200L)/1000.0;
		double w_ytd=300000.00;
		row->set_value(W_TAX, tax);
		row->set_value(W_YTD, w_ytd);

		index_insert(i_warehouse, wid, row, wh_to_part(wid));
	}

	return;
}

void TPCCWorkload::init_tab_dist(uint64_t wid) {
	for (uint64_t did = 1; did <= g_dist_per_wh; did++) {
		row_t * row;
		uint64_t row_id;
		t_district->get_new_row(row, 0, row_id);
		row->set_primary_key(did);

		row->set_value(D_ID, did);
		row->set_value(D_W_ID, wid);
		char name[10];
		MakeAlphaString(6, 10, name);
		row->set_value(D_NAME, name);
		char street[20];
		MakeAlphaString(10, 20, street);
		row->set_value(D_STREET_1, street);
		MakeAlphaString(10, 20, street);
		row->set_value(D_STREET_2, street);
		MakeAlphaString(10, 20, street);
		row->set_value(D_CITY, street);
		char state[2];
		MakeAlphaString(2, 2, state); /* State */
		row->set_value(D_STATE, state);
		char zip[9];
		MakeNumberString(9, 9, zip); /* Zip */
		row->set_value(D_ZIP, zip);
		double tax = (double)URand(0L,200L)/1000.0;
		double w_ytd=30000.00;
		row->set_value(D_TAX, tax);
		row->set_value(D_YTD, w_ytd);
		row->set_value(D_NEXT_O_ID, 3001);

		index_insert(i_district, distKey(did, wid), row, wh_to_part(wid));
	}
}

void TPCCWorkload::init_tab_stock(int id, uint64_t wid) {

	for (UInt32 sid = id + 1; sid <= g_max_items; sid+=g_init_parallelism) {
		row_t * row;
		uint64_t row_id;
		t_stock->get_new_row(row, 0, row_id);
		row->set_primary_key(sid);
		row->set_value(S_I_ID, sid);
		row->set_value(S_W_ID, wid);
		row->set_value(S_QUANTITY, URand(10, 100));
		row->set_value(S_REMOTE_CNT, 0);
#if !TPCC_SMALL
		char s_dist[25];
		char row_name[10] = "S_DIST_";
		for (int i = 1; i <= 10; i++) {
			if (i < 10) {
				row_name[7] = '0';
				row_name[8] = i + '0';
			} else {
				row_name[7] = '1';
				row_name[8] = '0';
			}
			row_name[9] = '\0';
			//MakeAlphaString(24, 24, s_dist);
			row->set_value(row_name, s_dist);
		}
		row->set_value(S_YTD, 0);
		row->set_value(S_ORDER_CNT, 0);
		char s_data[50];
	/*
		int len = MakeAlphaString(26, 50, s_data);
		if (rand() % 100 < 10) {
			int idx = URand(0, len - 8);
			strcpy(&s_data[idx], "original");
		}
	*/
		row->set_value(S_DATA, s_data);
#endif
		index_insert(i_stock, stockKey(sid, wid), row, wh_to_part(wid));
	}
}

void TPCCWorkload::init_tab_cust(int id, uint64_t did, uint64_t wid) {
	assert(g_cust_per_dist >= 1000);
	for (UInt32 cid = id+1; cid <= g_cust_per_dist; cid += g_init_parallelism) {
		row_t * row;
		uint64_t row_id;
		t_customer->get_new_row(row, 0, row_id);
		row->set_primary_key(cid);

		row->set_value(C_ID, cid);
		row->set_value(C_D_ID, did);
		row->set_value(C_W_ID, wid);
		char c_last[LASTNAME_LEN];
		if (cid <= 1000)
			Lastname(cid - 1, c_last);
		else
			Lastname(NURand(255,0,999), c_last);
		row->set_value(C_LAST, c_last);
	/*
#if !TPCC_SMALL
		char tmp[2];
	tmp[0] = 'O';
	tmp[1] = 'E';
		row->set_value(C_MIDDLE, tmp);
		char c_first[FIRSTNAME_LEN];
		MakeAlphaString(FIRSTNAME_MINLEN, sizeof(c_first), c_first);
		row->set_value(C_FIRST, c_first);
		char street[20];
		MakeAlphaString(10, 20, street);
		row->set_value(C_STREET_1, street);
		MakeAlphaString(10, 20, street);
		row->set_value(C_STREET_2, street);
		MakeAlphaString(10, 20, street);
		row->set_value(C_CITY, street);
		char state[2];
		MakeAlphaString(2, 2, state); // State
		row->set_value(C_STATE, state);
		char zip[9];
		MakeNumberString(9, 9, zip); // Zip
		row->set_value(C_ZIP, zip);
		char phone[16];
  		MakeNumberString(16, 16, phone); // Zip
		row->set_value(C_PHONE, phone);
		row->set_value(C_SINCE, 0);
		row->set_value(C_CREDIT_LIM, 50000);
		row->set_value(C_DELIVERY_CNT, 0);
		char c_data[500];
		MakeAlphaString(300, 500, c_data);
		row->set_value(C_DATA, c_data);

#endif
*/
		if (RAND(10) == 0) {
			char tmp[] = "GC";
			row->set_value(C_CREDIT, tmp);
		} else {
			char tmp[] = "BC";
			row->set_value(C_CREDIT, tmp);
		}
		row->set_value(C_DISCOUNT, (double)RAND(5000) / 10000);
		row->set_value(C_BALANCE, -10.0);
		row->set_value(C_YTD_PAYMENT, 10.0);
		row->set_value(C_PAYMENT_CNT, 1);
		uint64_t key;
		key = custNPKey(c_last, did, wid);
		index_insert(i_customer_last, key, row, wh_to_part(wid));
		key = custKey(cid, did, wid);
		index_insert(i_customer_id, key, row, wh_to_part(wid));
	}
}

void TPCCWorkload::init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id) {
	row_t * row;
	uint64_t row_id;
	t_history->get_new_row(row, 0, row_id);
	row->set_primary_key(0);
	row->set_value(H_C_ID, c_id);
	row->set_value(H_C_D_ID, d_id);
	row->set_value(H_D_ID, d_id);
	row->set_value(H_C_W_ID, w_id);
	row->set_value(H_W_ID, w_id);
	row->set_value(H_DATE, 0);
	row->set_value(H_AMOUNT, 10.0);
#if !TPCC_SMALL
	char h_data[24];
	//MakeAlphaString(12, 24, h_data);
	row->set_value(H_DATA, h_data);
#endif

}

void TPCCWorkload::init_tab_order(int id, uint64_t did, uint64_t wid) {
	init_permutation(); /* initialize permutation of customer numbers */
	for (UInt32 oid = id+1; oid <= g_cust_per_dist; oid+=g_init_parallelism) {
		row_t * row;
		uint64_t row_id;
		t_order->get_new_row(row, 0, row_id);
		row->set_primary_key(oid);
		uint64_t o_ol_cnt = 1;
		uint64_t cid = get_permutation();
		row->set_value(O_ID, oid);
		row->set_value(O_C_ID, cid);
		row->set_value(O_D_ID, did);
		row->set_value(O_W_ID, wid);
		uint64_t o_entry = 2013;
		row->set_value(O_ENTRY_D, o_entry);
		if (oid < 2101)
			row->set_value(O_CARRIER_ID, URand(1, 10));
		else
			row->set_value(O_CARRIER_ID, 0);
		o_ol_cnt = URand(5, 15);
		row->set_value(O_OL_CNT, o_ol_cnt);
		row->set_value(O_ALL_LOCAL, 1);

		// Insert to indexes
//		uint64_t key = custKey(cid, did, wid);
//		index_insert(i_order_wdc, key, row, wh_to_part(wid));

//		key = orderPrimaryKey(wid, did, oid);
//		index_insert(i_order_wdo, key, row, wh_to_part(wid));

		// ORDER-LINE
#if !TPCC_SMALL
		for (uint64_t ol = 1; ol <= o_ol_cnt; ol++) {
			t_orderline->get_new_row(row, 0, row_id);
			row->set_value(OL_O_ID, oid);
			row->set_value(OL_D_ID, did);
			row->set_value(OL_W_ID, wid);
			row->set_value(OL_NUMBER, ol);
			row->set_value(OL_I_ID, URand(1, 100000));
			row->set_value(OL_SUPPLY_W_ID, wid);
			if (oid < 2101) {
				row->set_value(OL_DELIVERY_D, o_entry);
				row->set_value(OL_AMOUNT, 0);
			} else {
				row->set_value(OL_DELIVERY_D, 0);
				row->set_value(OL_AMOUNT, (double)URand(1, 999999)/100);
			}
			row->set_value(OL_QUANTITY, 5);
			char ol_dist_info[24];
			MakeAlphaString(24, 24, ol_dist_info);
			row->set_value(OL_DIST_INFO, ol_dist_info);

//			uint64_t key = orderlineKey(wid, did, oid);
//			index_insert(i_orderline, key, row, wh_to_part(wid));

//			key = distKey(did, wid);
//			index_insert(i_orderline_wd, key, row, wh_to_part(wid));
		}
#endif
		// NEW ORDER
		if (oid > 2100) {
			t_neworder->get_new_row(row, 0, row_id);
			row->set_value(NO_O_ID, oid);
			row->set_value(NO_D_ID, did);
			row->set_value(NO_W_ID, wid);
		}
	}
}

/*==================================================================+
| ROUTINE NAME
| InitPermutation
+==================================================================*/

void TPCCWorkload::init_permutation() {
	UInt32 i;
	perm_count = 0;
	perm_c_id = new uint64_t[g_cust_per_dist];
	// Init with consecutive values
	for(i = 0; i < g_cust_per_dist; i++) {
		perm_c_id[i] = i+1;
	}

	// shuffle
	for(i=0; i < g_cust_per_dist-1; i++) {
		uint64_t j = URand(i+1, g_cust_per_dist-1);
		uint64_t tmp = perm_c_id[i];
		perm_c_id[i] = perm_c_id[j];
		perm_c_id[j] = tmp;
	}
	return;
}


/*==================================================================+
| ROUTINE NAME
| GetPermutation
+==================================================================*/

uint64_t TPCCWorkload::get_permutation() {
	if(perm_count >= g_cust_per_dist) {
		// wrapped around, restart at 0
		perm_count = 0;
	}
	return (uint64_t) perm_c_id[perm_count++];
}

void * TPCCWorkload::threadInitItem(void * This) {
  TPCCWorkload * wl = ((thr_args*) This)->wl;
  int id = ((thr_args*) This)->id;
	wl->init_tab_item(id);
	printf("ITEM Done\n");
	return NULL;
}

void * TPCCWorkload::threadInitWh(void * This) {
  TPCCWorkload * wl = (TPCCWorkload*) This;
	wl->init_tab_wh();
	printf("WAREHOUSE Done\n");
	return NULL;
}

void * TPCCWorkload::threadInitDist(void * This) {
  TPCCWorkload * wl = (TPCCWorkload*) This;
	for (uint64_t wid = 1; wid <= g_num_wh; wid ++) {
	if (GET_NODE_ID(wh_to_part(wid)) != g_node_id) continue;
		wl->init_tab_dist(wid);
  }
	printf("DISTRICT Done\n");
	return NULL;
}

void * TPCCWorkload::threadInitStock(void * This) {
  TPCCWorkload * wl = ((thr_args*) This)->wl;
  int id = ((thr_args*) This)->id;
	for (uint64_t wid = 1; wid <= g_num_wh; wid ++) {
	if (GET_NODE_ID(wh_to_part(wid)) != g_node_id) continue;
		wl->init_tab_stock(id,wid);
  }
	printf("STOCK Done\n");
	return NULL;
}

void * TPCCWorkload::threadInitCust(void * This) {
  TPCCWorkload * wl = ((thr_args*) This)->wl;
  int id = ((thr_args*) This)->id;
	for (uint64_t wid = 1; wid <= g_num_wh; wid ++) {
	if (GET_NODE_ID(wh_to_part(wid)) != g_node_id) continue;
		for (uint64_t did = 1; did <= g_dist_per_wh; did++) {
			wl->init_tab_cust(id,did, wid);
	}
  }
	printf("CUSTOMER %d Done\n",((thr_args *)This)->id);
	return NULL;
}

void * TPCCWorkload::threadInitHist(void * This) {
  TPCCWorkload * wl = (TPCCWorkload*) This;
	for (uint64_t wid = 1; wid <= g_num_wh; wid ++) {
	if (GET_NODE_ID(wh_to_part(wid)) != g_node_id) continue;
		for (uint64_t did = 1; did <= g_dist_per_wh; did++)
	  for (uint64_t cid = 1; cid <= g_cust_per_dist; cid++) wl->init_tab_hist(cid, did, wid);
  }
	printf("HISTORY Done\n");
	return NULL;
}

void * TPCCWorkload::threadInitOrder(void * This) {
  TPCCWorkload * wl = ((thr_args*) This)->wl;
  int id = ((thr_args*) This)->id;
	for (uint64_t wid = 1; wid <= g_num_wh; wid ++) {
	if (GET_NODE_ID(wh_to_part(wid)) != g_node_id) continue;
	for (uint64_t did = 1; did <= g_dist_per_wh; did++) wl->init_tab_order(id, did, wid);
  }
	printf("ORDER Done\n");
	return NULL;
}



