count = -1
mdb = dict()
win = 10

def ts():
	global count
	count += 1
	return count


def generate_timestamps(t_o):
	t_s = dict()
	for i in t_o:
		t_s[i] = ts()
	return t_s


def create_mvdb(it):
	if it not in mdb.keys():
		mdb[it] = []


def read(it, tran_rts):
	create_mvdb(it)
	if len(mdb[it]) == 0:
		return -2
	else:
		x = -1
		for ii in reversed(range(len(mdb[it]))):
			if tran_rts >= mdb[it][ii]['w']:
				break
			x = ii
		# mdb[it][-1]['r'] = max(tran_rts, mdb[it][-1]['r'])
				# Successful
	# -1 if x is abort
	return x


def write(it, trans):
	create_mvdb(it)
	if len(mdb[it]) == 0:
		temp = dict()
		temp['r'] = temp['w'] = trans[1]
		temp['t'] = trans[0]
		mdb[it].append(temp)
		return 0
	else:
		if trans[1] >= mdb[it][-1]['r']:
			temp = dict()
			temp['r'] = temp['w'] = trans[1]
			temp['t'] = trans[0]
			mdb[it].append(temp)
			mdb[it] = mdb[it][-win : ]
			# Successful
			return 0
	# Abort
	return -1


# def rollback(it, tran_wts):
# 	rbacks = []
# 	# print("ROLL", tran_wts)
# 	while  len(mdb[it]):
# 		if mdb[it][-1]['r'] > tran_wts:
# 			rbacks.append(mdb[it][-1]['t'])
# 			del mdb[it][-1]
# 	return rbacks

def rollback(t):
	for it in mdb.keys():
		for vs in mdb[it]:
			if vs['t'] == t:
				mdb[it].remove(vs)

# R : 1
# W : 1
# C : 3

transaction_orders = list([0, 1, 2, 3])
transaction_order = [i for i in range(len(transaction_orders))]
transaction_timestamps = generate_timestamps(transaction_order)

# ####################################################
# transaction_finish_order_wait = list([])
# # Wait
# # Transactions with lesser timestamps must finish first
# table = list(
# [
# 	[(1, 1), (-1, -1), (0, 1), (-1, -1)],
# 	[(2, -1), (0, 1), (-1, -1), (-1, -1)],
# 	[(-1, -1), (-1, -1), (-1, -1), (1, 1)],
# 	[(-1, -1), (-1, -1), (1, 1), (2, -1)],
# 	[(-1, -1), (1, 1), (2, -1), (-1, -1)],
# 	[(-1, -1), (2, -1), (-1, -1), (-1, -1)],
# ])
# waiting_queue = []
# for t in transaction_order:
# 	for row in range(len(table)-1):
# 		if table[row][t][0] == 1:
# 			waiting_queue.append(t)
# 			break
#
# while len(waiting_queue):
# 	waiting_queue.pop(0)
# 	for row in range(len(table)-1):
# 		for t in transaction_order:
# 			if t not in waiting_queue and t not in transaction_finish_order_wait:
# 				operation = table[row][t][0]
# 				item = table[row][t][1]
# 				if operation == -1 or operation == 2:
# 					pass
# 				else:
# 					if operation == 0:
# 						read(item, transaction_timestamps[t])
# 						print("transaction: %d, row: %d, operation: %s, item: %d"%(t, row, "READS", item))
# 					if operation == 1:
# 						res = write(item, [t, transaction_timestamps[t]])
# 						print("transaction: %d, row: %d, operation: %s, item: %d"%(t, row, "WRITES", item))
# 					if table[row + 1][t][0] == 2 and t not in transaction_finish_order_wait:
# 						transaction_finish_order_wait.append(t)
# print(transaction_finish_order_wait)
#
# ####################################################
# #
# # Partial roll back
# # Transactions with lesser timestamps must finish first
#
# table_cp = list(
# [
# 	[(1, 1), (-1, -1)],
# 	[(0, 1), (-1, -1)],
# 	[(3, 1), (-1, -1)],
# 	[(-1, -1), (1, 2)],
# 	[(1, 2), (-1, -1)],
# 	[(2, -1), (0, 2)],
# 	[(-1, -1), (2, -1)]
# ]
# )
#
# items = [1, 2]
#
# def create_check_point(it):
# 	create_mvdb(it)
# 	temp = dict()
# 	temp['r'] = temp['w'] = temp['t'] = -1
# 	mdb[it] = [temp]
#
# transaction_order = [0, 1]
# transaction_finish_order_roll = []
# not_completed = transaction_order[:]
# do_not_consider = []
#
# print("Order in which it is done: ")
#
# while len(not_completed):
# 	row = 0
# 	while row < len(table_cp) - 1:
# 		for t in transaction_order:
# 			if t not in transaction_finish_order_roll and t not in do_not_consider:
# 				operation = table_cp[row][t][0]
# 				item = table_cp[row][t][1]
# 				res = 1
# 				if operation == -1 or operation == 2:
# 					pass
# 				else:
# 					if operation == 0:
# 						read(item, transaction_timestamps[t])
# 						print("transaction: %d, row: %d, operation: %s, item: %d"%(t, row, "READS", item))
# 					elif operation == 1:
# 						res = write(item, [t, transaction_timestamps[t]])
# 						print("transaction: %d, row: %d, operation: %s, item: %d"%(t, row, "WRITE", item))
# 						if res == -1:
# 							r = rollback(t)
# 							transaction_timestamps[t] = ts()
# 							do_not_consider.append(t)
# 							print("transaction: %d, row: %d, operation: %s, item: %d" % (t, row, "ROLLBACK", item))
# 					elif operation == 3:
# 						print("transaction: %d, row: %d, operation: %s"%(t, row, "CHECKPOINT"))
# 						for r in range(row+1):
# 							table_cp[r][t] = (-1, -1)
# 						create_check_point(item)
# 					if table_cp[row + 1][t][0] == 2 and t not in transaction_finish_order_roll and res != -1:
# 						print("transaction: %d, row: %d, operation: %s, item: %d"%(t, row, "COMMIT", item))
# 						transaction_finish_order_roll.append(t)
# 						not_completed.remove(t)
# 			not_completed = list(set([*not_completed, *do_not_consider]))
# 			do_not_consider = []
# 		row += 1
#
# print(transaction_finish_order_roll)
# ####################################################
# Roll Multi items
# table_multi = list(
# [
# 	[(1, 1), (-1, -1), (0, 1), (-1, -1)],
# 	[(2, -1), (1, 2), (-1, -1), (-1, -1)],
# 	[(-1, -1), (0, 1), (1, 2), (1, 1)],
# 	[(-1, -1), (-1, -1), (1, 1), (2, -1)],
# 	[(-1, -1), (1, 1), (2, -1), (-1, -1)],
# 	[(-1, -1), (2, -1), (-1, -1), (-1, -1)],
# ])
#
# transaction_finish_order_wait_multi = list([])
# # Wait
# # Transactions with lesser timestamps must finish first
# waiting_queue = []
# for t in transaction_order:
# 	for row in range(len(table_multi)-1):
# 		if table_multi[row][t][0] == 1:
# 			waiting_queue.append(t)
# 			break
#
# while len(waiting_queue):
# 	waiting_queue.pop(0)
# 	for row in range(len(table_multi)-1):
# 		for t in transaction_order:
# 			if t not in waiting_queue and t not in transaction_finish_order_wait_multi:
# 				operation = table_multi[row][t][0]
# 				item = table_multi[row][t][1]
# 				if operation == -1 or operation == 2:
# 					pass
# 				else:
# 					if operation == 0:
# 						read(item, transaction_timestamps[t])
# 						print("transaction: %d, row: %d, operation: %s, item: %d"%(t, row, "READS", item))
# 					if operation == 1:
# 						res = write(item, [t, transaction_timestamps[t]])
# 						print("transaction: %d, row: %d, operation: %s, item: %d"%(t, row, "WRITES", item))
# 					if table_multi[row + 1][t][0] == 2 and t not in transaction_finish_order_wait_multi:
# 						transaction_finish_order_wait_multi.append(t)
# print(transaction_finish_order_wait_multi)
# ####################################################
