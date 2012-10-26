/*************************************************************************************************
 * Ruby binding
 *                                                      Copyright (C) 2009-2010 Mikio Hirabayashi
 * This file is part of Kyoto Cabinet.
 * This program is free software: you can redistribute it and/or modify it under the terms of
 * the GNU General Public License as published by the Free Software Foundation, either version
 * 3 of the License, or any later version.
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 *************************************************************************************************/

#define __STDC_LIMIT_MACROS
#include <kcpolydb.h>

namespace kc = kyotocabinet;

extern "C" {

#include <ruby.h>


typedef std::map<std::string, std::string> StringMap;
typedef VALUE (*METHOD)(...);


const int32_t VISMAGICNOP = INT32_MAX / 4 + 0;
const int32_t VISMAGICREMOVE = INT32_MAX / 4 + 1;


VALUE mod_kc;
VALUE cls_th;
VALUE cls_mtx;
VALUE cls_ex_arg;
VALUE cls_err;
VALUE cls_vis;
VALUE cls_vis_magic;
VALUE cls_fproc;
VALUE cls_cur;
VALUE cls_db;
ID id_th_pass;
ID id_mtx_lock;
ID id_mtx_unlock;
ID id_err_to_s;
ID id_vis_magic;
ID id_vis_nop;
ID id_vis_remove;
ID id_vis_visit_full;
ID id_vis_visit_empty;
ID id_fproc_process;
ID id_cur_db;
ID id_db_error;
ID id_db_open;
ID id_db_close;
ID id_db_mutex;


void Init_kyotocabinet();
static volatile VALUE StringValueEx(VALUE vobj);
static VALUE maptovhash(const StringMap* map);
static void init_env();
static void th_pass();
static void init_err();
static VALUE err_new(VALUE cls);
static void err_del(void* ptr);
static VALUE err_initialize(int argc, VALUE* argv, VALUE vself);
static VALUE err_set(VALUE vself, VALUE vcode, VALUE vmsg);
static VALUE err_code(VALUE vself);
static VALUE err_name(VALUE vself);
static VALUE err_message(VALUE vself);
static VALUE err_to_s(VALUE vself);
static void init_vis();
static VALUE vis_magic_initialize(VALUE vself, VALUE vnum);
static VALUE vis_visit_full(VALUE vself, VALUE vkey, VALUE vvalue);
static VALUE vis_visit_empty(VALUE vself, VALUE vkey);
static void init_fproc();
static VALUE fproc_process(VALUE vself, VALUE vpath);
static void init_cur();
static VALUE cur_new(VALUE cls);
static void cur_del(void* ptr);
static VALUE cur_initialize(VALUE vself, VALUE vdb);
static VALUE cur_disable(VALUE vself);
static VALUE cur_accept(int argc, VALUE* argv, VALUE vself);
static VALUE cur_get_key(VALUE vself);
static VALUE cur_get_value(VALUE vself);
static VALUE cur_get(VALUE vself);
static VALUE cur_remove(VALUE vself);
static VALUE cur_jump(int argc, VALUE* argv, VALUE vself);
static VALUE cur_step(VALUE vself);
static VALUE cur_db(VALUE vself);
static VALUE cur_error(VALUE vself);
static VALUE cur_to_s(VALUE vself);
static void init_db();
static VALUE db_new(VALUE cls);
static void db_del(void* ptr);
static void db_lock(VALUE vdb);
static void db_unlock(VALUE vdb);
static VALUE db_initialize(VALUE vself);
static VALUE db_error(VALUE vself);
static VALUE db_open(int argc, VALUE* argv, VALUE vself);
static VALUE db_close(VALUE vself);
static VALUE db_accept(int argc, VALUE* argv, VALUE vself);
static VALUE db_iterate(int argc, VALUE* argv, VALUE vself);
static VALUE db_set(VALUE vself, VALUE vkey, VALUE vvalue);
static VALUE db_add(VALUE vself, VALUE vkey, VALUE vvalue);
static VALUE db_append(VALUE vself, VALUE vkey, VALUE vvalue);
static VALUE db_increment(int argc, VALUE* argv, VALUE vself);
static VALUE db_cas(VALUE vself, VALUE vkey, VALUE voval, VALUE vnval);
static VALUE db_remove(VALUE vself, VALUE vkey);
static VALUE db_get(VALUE vself, VALUE vkey);
static VALUE db_clear(VALUE vself);
static VALUE db_synchronize(int argc, VALUE* argv, VALUE vself);
static VALUE db_begin_transaction(int argc, VALUE* argv, VALUE vself);
static VALUE db_end_transaction(int argc, VALUE* argv, VALUE vself);
static VALUE db_count(VALUE vself);
static VALUE db_size(VALUE vself);
static VALUE db_path(VALUE vself);
static VALUE db_status(VALUE vself);
static VALUE db_cursor(VALUE vself);
static VALUE db_to_s(VALUE vself);
static VALUE db_each(VALUE vself);
static VALUE db_each_key(VALUE vself);
static VALUE db_each_value(VALUE vself);
static VALUE db_process(int argc, VALUE* argv, VALUE vself);
static VALUE db_process_body(VALUE args);
static VALUE db_process_ensure(VALUE args);


class CursorBurrow {
private:
  typedef std::vector<kc::PolyDB::Cursor*> CursorList;
public:
  CursorBurrow() : dcurs_() {}
  ~CursorBurrow() {
    sweap();
  }
  void sweap() {
    if (dcurs_.size() > 0) {
      CursorList::iterator dit = dcurs_.begin();
      CursorList::iterator ditend = dcurs_.end();
      while (dit != ditend) {
        kc::PolyDB::Cursor* cur = *dit;
        delete cur;
        dit++;
      }
      dcurs_.clear();
    }
  }
  void deposit(kc::PolyDB::Cursor* cur) {
    dcurs_.push_back(cur);
  }
private:
  CursorList dcurs_;
} g_curbur;


struct SoftCursor {
  kc::PolyDB::Cursor* cur_;
  SoftCursor() : cur_(NULL) {}
  ~SoftCursor() {
    if (cur_) g_curbur.deposit(cur_);
  }
};


class SoftVisitor : public kc::PolyDB::Visitor {
public:
  SoftVisitor(VALUE vvisitor) : vvisitor_(vvisitor) {}
private:
  const char* visit_full(const char* kbuf, size_t ksiz,
                         const char* vbuf, size_t vsiz, size_t* sp) {
    VALUE vkey = rb_str_new(kbuf, ksiz);
    VALUE vvalue = rb_str_new(vbuf, vsiz);
    VALUE args = rb_ary_new3(3, vvisitor_, vkey, vvalue);
    int result = 0;
    VALUE vrv = rb_protect(visit_full_impl, args, &result);
    const char* rv;
    if (result) {
      rv = NOP;
    } else if (rb_obj_is_kind_of(vrv, cls_vis_magic)) {
      VALUE vmagic = rb_ivar_get(vrv, id_vis_magic);
      int32_t num = NUM2INT(vmagic);
      if (num == VISMAGICREMOVE) {
        rv = kc::PolyDB::Visitor::REMOVE;
      } else {
        rv = kc::PolyDB::Visitor::NOP;
      }
    } else if (vrv == Qnil || vrv == Qfalse) {
      rv = NOP;
    } else {
      vrv = StringValueEx(vrv);
      rv = RSTRING_PTR(vrv);
      *sp = RSTRING_LEN(vrv);
    }
    return rv;
  }
  const char* visit_empty(const char* kbuf, size_t ksiz, size_t* sp) {
    VALUE vkey = rb_str_new(kbuf, ksiz);
    VALUE args = rb_ary_new3(2, vvisitor_, vkey);
    int result = 0;
    VALUE vrv = rb_protect(visit_empty_impl, args, &result);
    const char* rv;
    if (result) {
      rv = NOP;
    } else if (rb_obj_is_instance_of(vrv, cls_vis_magic)) {
      VALUE vmagic = rb_ivar_get(vrv, id_vis_magic);
      int32_t num = NUM2INT(vmagic);
      if (num == VISMAGICREMOVE) {
        rv = kc::PolyDB::Visitor::REMOVE;
      } else {
        rv = kc::PolyDB::Visitor::NOP;
      }
    } else if (vrv == Qnil || vrv == Qfalse) {
      rv = NOP;
    } else {
      vrv = StringValueEx(vrv);
      rv = RSTRING_PTR(vrv);
      *sp = RSTRING_LEN(vrv);
    }
    return rv;
  }
  static VALUE visit_full_impl(VALUE args) {
    VALUE vvisitor = rb_ary_shift(args);
    VALUE vkey = rb_ary_shift(args);
    VALUE vvalue = rb_ary_shift(args);
    return rb_funcall(vvisitor, id_vis_visit_full, 2, vkey, vvalue);
  }
  static VALUE visit_empty_impl(VALUE args) {
    VALUE vvisitor = rb_ary_shift(args);
    VALUE vkey = rb_ary_shift(args);
    return rb_funcall(vvisitor, id_vis_visit_empty, 1, vkey);
  }
  VALUE vvisitor_;
};


class SoftEachVisitor : public kc::PolyDB::Visitor {
private:
  const char* visit_full(const char* kbuf, size_t ksiz,
                         const char* vbuf, size_t vsiz, size_t* sp) {
    VALUE vkey = rb_str_new(kbuf, ksiz);
    VALUE vvalue = rb_str_new(vbuf, vsiz);
    VALUE args = rb_ary_new3(2, vkey, vvalue);
    int result = 0;
    rb_protect(visit_full_impl, args, &result);
    return NOP;
  }
  static VALUE visit_full_impl(VALUE args) {
    return rb_yield(args);
  }
};


class SoftEachKeyVisitor : public kc::PolyDB::Visitor {
private:
  const char* visit_full(const char* kbuf, size_t ksiz,
                         const char* vbuf, size_t vsiz, size_t* sp) {
    VALUE vkey = rb_str_new(kbuf, ksiz);
    VALUE args = rb_ary_new3(1, vkey);
    int result = 0;
    rb_protect(visit_full_impl, args, &result);
    return NOP;
  }
  static VALUE visit_full_impl(VALUE args) {
    return rb_yield(args);
  }
};


class SoftEachValueVisitor : public kc::PolyDB::Visitor {
private:
  const char* visit_full(const char* kbuf, size_t ksiz,
                         const char* vbuf, size_t vsiz, size_t* sp) {
    VALUE vvalue = rb_str_new(vbuf, vsiz);
    VALUE args = rb_ary_new3(1, vvalue);
    int result = 0;
    rb_protect(visit_full_impl, args, &result);
    return NOP;
  }
  static VALUE visit_full_impl(VALUE args) {
    return rb_yield(args);
  }
};


class SoftFileProcessor : public kc::PolyDB::FileProcessor {
public:
  SoftFileProcessor(VALUE vproc) : vproc_(vproc) {}
private:
  bool process(const std::string& path, int64_t count, int64_t size) {
    VALUE vpath = rb_str_new2(path.c_str());
    VALUE args = rb_ary_new3(2, vproc_, vpath);
    rb_ary_push(args, INT2FIX(count));
    rb_ary_push(args, INT2FIX(size));
    int result = 0;
    VALUE vrv = rb_protect(process_impl, args, &result);
    return !result && vrv != Qnil && vrv != Qfalse;
  }
  static VALUE process_impl(VALUE args) {
    VALUE vproc = rb_ary_shift(args);
    VALUE vpath = rb_ary_shift(args);
    VALUE vcount = rb_ary_shift(args);
    VALUE vsize = rb_ary_shift(args);
    return rb_funcall(vproc, id_fproc_process, 3, vpath, vcount, vsize);
  }
  VALUE vproc_;
};


void Init_kyotocabinet() {
  mod_kc = rb_define_module("KyotoCabinet");
  rb_define_const(mod_kc, "VERSION", rb_str_new2(kc::VERSION));
  init_env();
  init_err();
  init_vis();
  init_fproc();
  init_cur();
  init_db();
}


static volatile VALUE StringValueEx(VALUE vobj) {
  char kbuf[kc::NUMBUFSIZ];
  switch (TYPE(vobj)) {
    case T_FIXNUM: {
      size_t ksiz = std::sprintf(kbuf, "%d", (int)FIX2INT(vobj));
      return rb_str_new(kbuf, ksiz);
    }
    case T_BIGNUM: {
      size_t ksiz = std::sprintf(kbuf, "%lld", (long long)NUM2LL(vobj));
      return rb_str_new(kbuf, ksiz);
    }
    case T_TRUE: {
      size_t ksiz = std::sprintf(kbuf, "true");
      return rb_str_new(kbuf, ksiz);
    }
    case T_FALSE: {
      size_t ksiz = std::sprintf(kbuf, "false");
      return rb_str_new(kbuf, ksiz);
    }
    case T_NIL: {
      size_t ksiz = std::sprintf(kbuf, "nil");
      return rb_str_new(kbuf, ksiz);
    }
    case T_SYMBOL: {
      return rb_str_new2(rb_id2name(SYM2ID(vobj)));
    }
  }
  return StringValue(vobj);
}


static VALUE maptovhash(const StringMap* map) {
  VALUE vhash = rb_hash_new();
  StringMap::const_iterator it = map->begin();
  StringMap::const_iterator itend = map->end();
  while (it != itend) {
    VALUE vkey = rb_str_new(it->first.c_str(), it->first.size());
    VALUE vvalue = rb_str_new(it->second.c_str(), it->second.size());
    rb_hash_aset(vhash, vkey, vvalue);
    it++;
  }
  return vhash;
}


static void init_env() {
  rb_require("thread");
  cls_th = rb_path2class("Thread");
  id_th_pass = rb_intern("pass");
  cls_mtx = rb_path2class("Mutex");
  id_mtx_lock = rb_intern("lock");
  id_mtx_unlock = rb_intern("unlock");
  cls_ex_arg = rb_path2class("ArgumentError");
}


static void th_pass() {
  rb_funcall(cls_th, id_th_pass, 0);
}


static void init_err() {
  cls_err = rb_define_class_under(mod_kc, "Error", rb_cObject);
  rb_define_alloc_func(cls_err, err_new);
  rb_define_const(cls_err, "SUCCESS", INT2FIX(kc::PolyDB::Error::SUCCESS));
  rb_define_const(cls_err, "NOIMPL", INT2FIX(kc::PolyDB::Error::NOIMPL));
  rb_define_const(cls_err, "INVALID", INT2FIX(kc::PolyDB::Error::INVALID));
  //rb_define_const(cls_err, "NOFILE", INT2FIX(kc::PolyDB::Error::NOFILE));
  rb_define_const(cls_err, "NOPERM", INT2FIX(kc::PolyDB::Error::NOPERM));
  rb_define_const(cls_err, "BROKEN", INT2FIX(kc::PolyDB::Error::BROKEN));
  rb_define_const(cls_err, "DUPREC", INT2FIX(kc::PolyDB::Error::DUPREC));
  rb_define_const(cls_err, "NOREC", INT2FIX(kc::PolyDB::Error::NOREC));
  rb_define_const(cls_err, "LOGIC", INT2FIX(kc::PolyDB::Error::LOGIC));
  rb_define_const(cls_err, "SYSTEM", INT2FIX(kc::PolyDB::Error::SYSTEM));
  rb_define_const(cls_err, "MISC", INT2FIX(kc::PolyDB::Error::MISC));
  rb_define_private_method(cls_err, "initialize", (METHOD)err_initialize, -1);
  rb_define_method(cls_err, "set", (METHOD)err_set, 2);
  rb_define_method(cls_err, "code", (METHOD)err_code, 0);
  rb_define_method(cls_err, "name", (METHOD)err_name, 0);
  rb_define_method(cls_err, "message", (METHOD)err_message, 0);
  rb_define_method(cls_err, "to_i", (METHOD)err_code, 0);
  rb_define_method(cls_err, "to_s", (METHOD)err_to_s, 0);
  id_err_to_s = rb_intern("to_s");
}


static VALUE err_new(VALUE cls) {
  kc::PolyDB::Error* err = new kc::PolyDB::Error();
  return Data_Wrap_Struct(cls_err, 0, err_del, err);
}


static void err_del(void* ptr) {
  delete (kc::PolyDB::Error*)ptr;
}


static VALUE err_initialize(int argc, VALUE* argv, VALUE vself) {
  kc::PolyDB::Error* err;
  Data_Get_Struct(vself, kc::PolyDB::Error, err);
  VALUE vcode, vmsg;
  rb_scan_args(argc, argv, "02", &vcode, &vmsg);
  int32_t code = vcode == Qnil ? kc::PolyDB::Error::SUCCESS : NUM2INT(vcode);
  const char* msg;
  if (vmsg == Qnil) {
    msg = "error";
  } else {
    vmsg = StringValueEx(vmsg);
    msg = RSTRING_PTR(vmsg);
  }
  err->set((kc::PolyDB::Error::Code)code, msg);
  return Qnil;
}


static VALUE err_set(VALUE vself, VALUE vcode, VALUE vmsg) {
  kc::PolyDB::Error* err;
  Data_Get_Struct(vself, kc::PolyDB::Error, err);
  int32_t code = NUM2INT(vcode);
  vmsg = StringValueEx(vmsg);
  const char* msg = RSTRING_PTR(vmsg);
  err->set((kc::PolyDB::Error::Code)code, msg);
  return Qnil;
}


static VALUE err_code(VALUE vself) {
  kc::PolyDB::Error* err;
  Data_Get_Struct(vself, kc::PolyDB::Error, err);
  return INT2FIX(err->code());
}


static VALUE err_name(VALUE vself) {
  kc::PolyDB::Error* err;
  Data_Get_Struct(vself, kc::PolyDB::Error, err);
  return rb_str_new2(err->name());
}


static VALUE err_message(VALUE vself) {
  kc::PolyDB::Error* err;
  Data_Get_Struct(vself, kc::PolyDB::Error, err);
  return rb_str_new2(err->message());
}


static VALUE err_to_s(VALUE vself) {
  kc::PolyDB::Error* err;
  Data_Get_Struct(vself, kc::PolyDB::Error, err);
  std::string str;
  kc::strprintf(&str, "%s: %s", err->name(), err->message());
  return rb_str_new2(str.c_str());
}


static void init_vis() {
  cls_vis = rb_define_class_under(mod_kc, "Visitor", rb_cObject);
  cls_vis_magic = rb_define_class_under(mod_kc, "VisitorMagic", rb_cObject);
  rb_define_private_method(cls_vis_magic, "initialize", (METHOD)vis_magic_initialize, 1);
  id_vis_magic = rb_intern("@magic_");
  VALUE vnopnum = INT2FIX(VISMAGICNOP);
  VALUE vnop = rb_class_new_instance(1, &vnopnum, cls_vis_magic);
  rb_define_const(cls_vis, "NOP", vnop);
  VALUE vremovenum = INT2FIX(VISMAGICREMOVE);
  VALUE vremove = rb_class_new_instance(1, &vremovenum, cls_vis_magic);
  rb_define_const(cls_vis, "REMOVE", vremove);
  rb_define_method(cls_vis, "visit_full", (METHOD)vis_visit_full, 2);
  rb_define_method(cls_vis, "visit_empty", (METHOD)vis_visit_empty, 1);
  id_vis_nop = rb_intern("NOP");
  id_vis_remove = rb_intern("REMOVE");
  id_vis_visit_full = rb_intern("visit_full");
  id_vis_visit_empty = rb_intern("visit_empty");
}


static VALUE vis_magic_initialize(VALUE vself, VALUE vnum) {
  rb_ivar_set(vself, id_vis_magic, vnum);
  return Qnil;
}


static VALUE vis_visit_full(VALUE vself, VALUE vkey, VALUE vvalue) {
  return rb_const_get(cls_vis, id_vis_nop);
}


static VALUE vis_visit_empty(VALUE vself, VALUE vkey) {
  return rb_const_get(cls_vis, id_vis_nop);
}


static void init_fproc() {
  cls_fproc = rb_define_class_under(mod_kc, "FileProcessor", rb_cObject);
  rb_define_method(cls_fproc, "process", (METHOD)fproc_process, 1);
  id_fproc_process = rb_intern("process");
}


static VALUE fproc_process(VALUE vself, VALUE vpath) {
  return Qtrue;
}


static void init_cur() {
  cls_cur = rb_define_class_under(mod_kc, "Cursor", rb_cObject);
  rb_define_alloc_func(cls_cur, cur_new);
  rb_define_private_method(cls_cur, "initialize", (METHOD)cur_initialize, 1);
  rb_define_method(cls_cur, "disable", (METHOD)cur_disable, 0);
  rb_define_method(cls_cur, "accept", (METHOD)cur_accept, -1);
  rb_define_method(cls_cur, "get_key", (METHOD)cur_get_key, 0);
  rb_define_method(cls_cur, "get_value", (METHOD)cur_get_value, 0);
  rb_define_method(cls_cur, "get", (METHOD)cur_get, 0);
  rb_define_method(cls_cur, "remove", (METHOD)cur_remove, 0);
  rb_define_method(cls_cur, "jump", (METHOD)cur_jump, -1);
  rb_define_method(cls_cur, "step", (METHOD)cur_step, 0);
  rb_define_method(cls_cur, "db", (METHOD)cur_db, 0);
  rb_define_method(cls_cur, "error", (METHOD)cur_error, 0);
  rb_define_method(cls_cur, "to_s", (METHOD)cur_to_s, 0);
  id_cur_db = rb_intern("@db_");
}


static VALUE cur_new(VALUE cls) {
  SoftCursor* cur = new SoftCursor;
  return Data_Wrap_Struct(cls_cur, 0, cur_del, cur);
}


static void cur_del(void* ptr) {
  delete (SoftCursor*)ptr;
}


static VALUE cur_initialize(VALUE vself, VALUE vdb) {
  SoftCursor* cur;
  Data_Get_Struct(vself, SoftCursor, cur);
  kc::PolyDB* db;
  Data_Get_Struct(vdb, kc::PolyDB, db);
  db_lock(vdb);
  g_curbur.sweap();
  cur->cur_ = db->cursor();
  db_unlock(vdb);
  rb_ivar_set(vself, id_cur_db, vdb);
  return Qnil;
}


static VALUE cur_disable(VALUE vself) {
  VALUE vdb = rb_ivar_get(vself, id_cur_db);
  if (vdb == Qnil) rb_raise(cls_ex_arg, "disabled cursor");
  SoftCursor* cur;
  Data_Get_Struct(vself, SoftCursor, cur);
  db_lock(vdb);
  delete cur->cur_;
  cur->cur_ = NULL;
  db_unlock(vdb);
  rb_ivar_set(vself, id_cur_db, Qnil);
  return Qnil;
}


static VALUE cur_accept(int argc, VALUE* argv, VALUE vself) {
  VALUE vdb = rb_ivar_get(vself, id_cur_db);
  if (vdb == Qnil) rb_raise(cls_ex_arg, "disabled cursor");
  SoftCursor* cur;
  Data_Get_Struct(vself, SoftCursor, cur);
  VALUE vvisitor, vwritable, vstep;
  rb_scan_args(argc, argv, "12", &vvisitor, &vwritable, &vstep);
  SoftVisitor visitor(vvisitor);
  bool writable = vwritable != Qfalse;
  bool step = vstep != Qnil && vstep != Qfalse;
  db_lock(vdb);
  bool rv = cur->cur_->accept(&visitor, writable, step);
  db_unlock(vdb);
  return rv ? Qtrue : Qfalse;
}


static VALUE cur_get_key(VALUE vself) {
  VALUE vdb = rb_ivar_get(vself, id_cur_db);
  if (vdb == Qnil) rb_raise(cls_ex_arg, "disabled cursor");
  SoftCursor* cur;
  Data_Get_Struct(vself, SoftCursor, cur);
  db_lock(vdb);
  size_t ksiz;
  char* kbuf = cur->cur_->get_key(&ksiz);
  db_unlock(vdb);
  VALUE vrv;
  if (kbuf) {
    vrv = rb_str_new(kbuf, ksiz);
    delete[] kbuf;
  } else {
    vrv = Qnil;
  }
  return vrv;
}


static VALUE cur_get_value(VALUE vself) {
  VALUE vdb = rb_ivar_get(vself, id_cur_db);
  if (vdb == Qnil) rb_raise(cls_ex_arg, "disabled cursor");
  SoftCursor* cur;
  Data_Get_Struct(vself, SoftCursor, cur);
  db_lock(vdb);
  size_t vsiz;
  char* vbuf = cur->cur_->get_value(&vsiz);
  db_unlock(vdb);
  VALUE vrv;
  if (vbuf) {
    vrv = rb_str_new(vbuf, vsiz);
    delete[] vbuf;
  } else {
    vrv = Qnil;
  }
  return vrv;
}


static VALUE cur_get(VALUE vself) {
  VALUE vdb = rb_ivar_get(vself, id_cur_db);
  if (vdb == Qnil) rb_raise(cls_ex_arg, "disabled cursor");
  SoftCursor* cur;
  Data_Get_Struct(vself, SoftCursor, cur);
  db_lock(vdb);
  size_t ksiz, vsiz;
  const char* vbuf;
  char* kbuf = cur->cur_->get(&ksiz, &vbuf, &vsiz);
  db_unlock(vdb);
  VALUE vrv;
  if (kbuf) {
    VALUE vkey = rb_str_new(kbuf, ksiz);
    VALUE vvalue = rb_str_new(vbuf, vsiz);
    vrv = rb_ary_new3(2, vkey, vvalue);
    delete[] kbuf;
  } else {
    vrv = Qnil;
  }
  return vrv;
}


static VALUE cur_remove(VALUE vself) {
  VALUE vdb = rb_ivar_get(vself, id_cur_db);
  if (vdb == Qnil) rb_raise(cls_ex_arg, "disabled cursor");
  SoftCursor* cur;
  Data_Get_Struct(vself, SoftCursor, cur);
  db_lock(vdb);
  bool rv = cur->cur_->remove();
  db_unlock(vdb);
  return rv ? Qtrue : Qfalse;
}


static VALUE cur_jump(int argc, VALUE* argv, VALUE vself) {
  VALUE vdb = rb_ivar_get(vself, id_cur_db);
  if (vdb == Qnil) rb_raise(cls_ex_arg, "disabled cursor");
  SoftCursor* cur;
  Data_Get_Struct(vself, SoftCursor, cur);
  VALUE vkey;
  rb_scan_args(argc, argv, "01", &vkey);
  VALUE vrv;
  if (vkey == Qnil) {
    db_lock(vdb);
    bool rv = cur->cur_->jump();
    db_unlock(vdb);
    vrv = rv ? Qtrue : Qfalse;
  } else {
    vkey = StringValueEx(vkey);
    const char* kbuf = RSTRING_PTR(vkey);
    size_t ksiz = RSTRING_LEN(vkey);
    db_lock(vdb);
    bool rv = cur->cur_->jump(kbuf, ksiz);
    db_unlock(vdb);
    vrv = rv ? Qtrue : Qfalse;
  }
  return vrv;
}


static VALUE cur_step(VALUE vself) {
  VALUE vdb = rb_ivar_get(vself, id_cur_db);
  if (vdb == Qnil) rb_raise(cls_ex_arg, "disabled cursor");
  SoftCursor* cur;
  Data_Get_Struct(vself, SoftCursor, cur);
  db_lock(vdb);
  bool rv = cur->cur_->step();
  db_unlock(vdb);
  return rv ? Qtrue : Qfalse;
}


static VALUE cur_db(VALUE vself) {
  VALUE vdb = rb_ivar_get(vself, id_cur_db);
  if (vdb == Qnil) rb_raise(cls_ex_arg, "disabled cursor");
  return vdb;
}


static VALUE cur_error(VALUE vself) {
  VALUE vdb = rb_ivar_get(vself, id_cur_db);
  if (vdb == Qnil) rb_raise(cls_ex_arg, "disabled cursor");
  SoftCursor* cur;
  Data_Get_Struct(vself, SoftCursor, cur);
  db_lock(vdb);
  kc::PolyDB::Error err = cur->cur_->error();
  db_unlock(vdb);
  VALUE args[2];
  args[0] = INT2FIX(err.code());
  args[1] = rb_str_new2(err.message());
  return rb_class_new_instance(2, args, cls_err);
}


static VALUE cur_to_s(VALUE vself) {
  VALUE vdb = rb_ivar_get(vself, id_cur_db);
  if (vdb == Qnil) rb_raise(cls_ex_arg, "disabled cursor");
  SoftCursor* cur;
  Data_Get_Struct(vself, SoftCursor, cur);
  db_lock(vdb);
  kc::PolyDB* db = cur->cur_->db();
  std::string path = db->path();
  std::string str;
  kc::strprintf(&str, "#<KyotoCabinet::Cursor:%p: %s", cur, path.c_str());
  size_t ksiz;
  char* kbuf = cur->cur_->get_key(&ksiz);
  if (kbuf) {
    kc::strprintf(&str, ":");
    str.append(kbuf, ksiz);
    delete[] kbuf;
  }
  kc::strprintf(&str, ">");
  db_unlock(vdb);
  return rb_str_new2(str.c_str());
}


static void init_db() {
  cls_db = rb_define_class_under(mod_kc, "DB", rb_cObject);
  rb_define_alloc_func(cls_db, db_new);
  rb_define_const(cls_db, "OREADER", INT2FIX(kc::PolyDB::OREADER));
  rb_define_const(cls_db, "OWRITER", INT2FIX(kc::PolyDB::OWRITER));
  rb_define_const(cls_db, "OCREATE", INT2FIX(kc::PolyDB::OCREATE));
  rb_define_const(cls_db, "OTRUNCATE", INT2FIX(kc::PolyDB::OTRUNCATE));
  rb_define_const(cls_db, "OAUTOTRAN", INT2FIX(kc::PolyDB::OAUTOTRAN));
  rb_define_const(cls_db, "OAUTOSYNC", INT2FIX(kc::PolyDB::OAUTOSYNC));
  rb_define_const(cls_db, "ONOLOCK", INT2FIX(kc::PolyDB::ONOLOCK));
  rb_define_const(cls_db, "OTRYLOCK", INT2FIX(kc::PolyDB::OTRYLOCK));
  rb_define_const(cls_db, "ONOREPAIR", INT2FIX(kc::PolyDB::ONOREPAIR));
  rb_define_private_method(cls_db, "initialize", (METHOD)db_initialize, 0);
  rb_define_method(cls_db, "error", (METHOD)db_error, 0);
  rb_define_method(cls_db, "open", (METHOD)db_open, -1);
  rb_define_method(cls_db, "close", (METHOD)db_close, 0);
  rb_define_method(cls_db, "accept", (METHOD)db_accept, -1);
  rb_define_method(cls_db, "iterate", (METHOD)db_iterate, -1);
  rb_define_method(cls_db, "set", (METHOD)db_set, 2);
  rb_define_method(cls_db, "add", (METHOD)db_add, 2);
  rb_define_method(cls_db, "append", (METHOD)db_append, 2);
  rb_define_method(cls_db, "increment", (METHOD)db_increment, -1);
  rb_define_method(cls_db, "cas", (METHOD)db_cas, 3);
  rb_define_method(cls_db, "remove", (METHOD)db_remove, 1);
  rb_define_method(cls_db, "get", (METHOD)db_get, 1);
  rb_define_method(cls_db, "clear", (METHOD)db_clear, 0);
  rb_define_method(cls_db, "synchronize", (METHOD)db_synchronize, -1);
  rb_define_method(cls_db, "begin_transaction", (METHOD)db_begin_transaction, -1);
  rb_define_method(cls_db, "end_transaction", (METHOD)db_end_transaction, -1);
  rb_define_method(cls_db, "count", (METHOD)db_count, 0);
  rb_define_method(cls_db, "size", (METHOD)db_size, 0);
  rb_define_method(cls_db, "path", (METHOD)db_path, 0);
  rb_define_method(cls_db, "status", (METHOD)db_status, 0);
  rb_define_method(cls_db, "cursor", (METHOD)db_cursor, 0);
  rb_define_method(cls_db, "to_s", (METHOD)db_to_s, 0);
  rb_define_method(cls_db, "[]", (METHOD)db_get, 1);
  rb_define_method(cls_db, "[]=", (METHOD)db_set, 2);
  rb_define_method(cls_db, "store", (METHOD)db_set, 2);
  rb_define_method(cls_db, "delete", (METHOD)db_set, 1);
  rb_define_method(cls_db, "fetch", (METHOD)db_set, 1);
  rb_define_method(cls_db, "length", (METHOD)db_count, 0);
  rb_define_method(cls_db, "each", (METHOD)db_each, 0);
  rb_define_method(cls_db, "each_pair", (METHOD)db_each, 0);
  rb_define_method(cls_db, "each_key", (METHOD)db_each_key, 0);
  rb_define_method(cls_db, "each_value", (METHOD)db_each_value, 0);
  id_db_error = rb_intern("error");
  id_db_open = rb_intern("open");
  id_db_close = rb_intern("close");
  id_db_mutex = rb_intern("@mutex_");
  rb_define_singleton_method(cls_db, "process", (METHOD)db_process, -1);
}


static VALUE db_new(VALUE cls) {
  kc::PolyDB* db = new kc::PolyDB();
  return Data_Wrap_Struct(cls_db, 0, db_del, db);
}


static void db_del(void* ptr) {
  g_curbur.sweap();
  delete (kc::PolyDB*)ptr;
}


static void db_lock(VALUE vdb) {
  VALUE vmutex = rb_ivar_get(vdb, id_db_mutex);
  rb_funcall(vmutex, id_mtx_lock, 0);
}


static void db_unlock(VALUE vdb) {
  VALUE vmutex = rb_ivar_get(vdb, id_db_mutex);
  rb_funcall(vmutex, id_mtx_unlock, 0);
}


static VALUE db_initialize(VALUE vself) {
  VALUE vmutex = rb_class_new_instance(0, NULL, cls_mtx);
  rb_ivar_set(vself, id_db_mutex, vmutex);
  return Qnil;
}


static VALUE db_error(VALUE vself) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  db_lock(vself);
  kc::PolyDB::Error err = db->error();
  db_unlock(vself);
  VALUE args[2];
  args[0] = INT2FIX(err.code());
  args[1] = rb_str_new2(err.message());
  return rb_class_new_instance(2, args, cls_err);
}


static VALUE db_open(int argc, VALUE* argv, VALUE vself) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  VALUE vpath, vmode;
  rb_scan_args(argc, argv, "02", &vpath, &vmode);
  if (vpath == Qnil) vpath = rb_str_new2("*");
  vpath = StringValueEx(vpath);
  const char* path = RSTRING_PTR(vpath);
  uint32_t mode = vmode == Qnil ? kc::PolyDB::OREADER : NUM2INT(vmode);
  db_lock(vself);
  bool rv = db->open(path, mode);
  db_unlock(vself);
  return rv ? Qtrue : Qfalse;
}


static VALUE db_close(VALUE vself) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  db_lock(vself);
  bool rv = db->close();
  db_unlock(vself);
  return rv ? Qtrue : Qfalse;
}


static VALUE db_accept(int argc, VALUE* argv, VALUE vself) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  VALUE vkey, vvisitor, vwritable;
  rb_scan_args(argc, argv, "21", &vkey, &vvisitor, &vwritable);
  vkey = StringValueEx(vkey);
  const char* kbuf = RSTRING_PTR(vkey);
  size_t ksiz = RSTRING_LEN(vkey);
  SoftVisitor visitor(vvisitor);
  bool writable = vwritable != Qfalse;
  db_lock(vself);
  bool rv = db->accept(kbuf, ksiz, &visitor, writable);
  db_unlock(vself);
  return rv ? Qtrue : Qfalse;
}


static VALUE db_iterate(int argc, VALUE* argv, VALUE vself) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  VALUE vvisitor, vwritable;
  rb_scan_args(argc, argv, "11", &vvisitor, &vwritable);
  SoftVisitor visitor(vvisitor);
  bool writable = vwritable != Qfalse;
  db_lock(vself);
  bool rv = db->iterate(&visitor, writable);
  db_unlock(vself);
  return rv ? Qtrue : Qfalse;
}


static VALUE db_set(VALUE vself, VALUE vkey, VALUE vvalue) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  vkey = StringValueEx(vkey);
  const char* kbuf = RSTRING_PTR(vkey);
  size_t ksiz = RSTRING_LEN(vkey);
  vvalue = StringValueEx(vvalue);
  const char* vbuf = RSTRING_PTR(vvalue);
  size_t vsiz = RSTRING_LEN(vvalue);
  db_lock(vself);
  bool rv = db->set(kbuf, ksiz, vbuf, vsiz);
  db_unlock(vself);
  return rv ? Qtrue : Qfalse;
}


static VALUE db_add(VALUE vself, VALUE vkey, VALUE vvalue) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  vkey = StringValueEx(vkey);
  const char* kbuf = RSTRING_PTR(vkey);
  size_t ksiz = RSTRING_LEN(vkey);
  vvalue = StringValueEx(vvalue);
  const char* vbuf = RSTRING_PTR(vvalue);
  size_t vsiz = RSTRING_LEN(vvalue);
  db_lock(vself);
  bool rv = db->add(kbuf, ksiz, vbuf, vsiz);
  db_unlock(vself);
  return rv ? Qtrue : Qfalse;
}


static VALUE db_append(VALUE vself, VALUE vkey, VALUE vvalue) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  vkey = StringValueEx(vkey);
  const char* kbuf = RSTRING_PTR(vkey);
  size_t ksiz = RSTRING_LEN(vkey);
  vvalue = StringValueEx(vvalue);
  const char* vbuf = RSTRING_PTR(vvalue);
  size_t vsiz = RSTRING_LEN(vvalue);
  db_lock(vself);
  bool rv = db->append(kbuf, ksiz, vbuf, vsiz);
  db_unlock(vself);
  return rv ? Qtrue : Qfalse;
}


static VALUE db_increment(int argc, VALUE* argv, VALUE vself) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  VALUE vkey, vnum;
  rb_scan_args(argc, argv, "11", &vkey, &vnum);
  vkey = StringValueEx(vkey);
  const char* kbuf = RSTRING_PTR(vkey);
  size_t ksiz = RSTRING_LEN(vkey);
  bool dmode = false;
  int64_t inum = 0;
  double dnum = NAN;
  switch (TYPE(vnum)) {
    case T_FIXNUM: {
      inum = FIX2INT(vnum);
      break;
    }
    case T_BIGNUM: {
      inum = NUM2LL(vnum);
      break;
    }
    case T_FLOAT: {
      dnum = NUM2DBL(vnum);
      dmode = true;
      break;
    }
    case T_TRUE: {
      inum = 1;
      break;
    }
    case T_STRING: {
      const char* str = RSTRING_PTR(vnum);
      if (std::strchr(str, '.')) {
        dnum = kc::atof(str);
        dmode = true;
      } else {
        inum = kc::atoi(str);
      }
      break;
    }
  }
  VALUE vrv;
  if (dmode) {
    db_lock(vself);
    double num = db->increment(kbuf, ksiz, dnum);
    db_unlock(vself);
    vrv = num == num ? rb_float_new(num) : Qnil;
  } else {
    db_lock(vself);
    int64_t num = db->increment(kbuf, ksiz, inum);
    db_unlock(vself);
    vrv = num == INT64_MIN ? Qnil : LL2NUM(num);
  }
  return vrv;
}


static VALUE db_cas(VALUE vself, VALUE vkey, VALUE voval, VALUE vnval) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  vkey = StringValueEx(vkey);
  const char* kbuf = RSTRING_PTR(vkey);
  size_t ksiz = RSTRING_LEN(vkey);
  const char* ovbuf = NULL;
  size_t ovsiz = 0;
  if (voval != Qnil) {
    voval = StringValueEx(voval);
    ovbuf = RSTRING_PTR(voval);
    ovsiz = RSTRING_LEN(voval);
  }
  const char* nvbuf = NULL;
  size_t nvsiz = 0;
  if (vnval != Qnil) {
    vnval = StringValueEx(vnval);
    nvbuf = RSTRING_PTR(vnval);
    nvsiz = RSTRING_LEN(vnval);
  }
  db_lock(vself);
  bool rv = db->cas(kbuf, ksiz, ovbuf, ovsiz, nvbuf, nvsiz);
  db_unlock(vself);
  return rv ? Qtrue : Qfalse;
}


static VALUE db_remove(VALUE vself, VALUE vkey) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  vkey = StringValueEx(vkey);
  const char* kbuf = RSTRING_PTR(vkey);
  size_t ksiz = RSTRING_LEN(vkey);
  db_lock(vself);
  bool rv = db->remove(kbuf, ksiz);
  db_unlock(vself);
  return rv ? Qtrue : Qfalse;
}


static VALUE db_get(VALUE vself, VALUE vkey) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  vkey = StringValueEx(vkey);
  const char* kbuf = RSTRING_PTR(vkey);
  size_t ksiz = RSTRING_LEN(vkey);
  db_lock(vself);
  size_t vsiz;
  char* vbuf = db->get(kbuf, ksiz, &vsiz);
  db_unlock(vself);
  VALUE vrv;
  if (vbuf) {
    vrv = rb_str_new(vbuf, vsiz);
    delete[] vbuf;
  } else {
    vrv = Qnil;
  }
  return vrv;
}


static VALUE db_clear(VALUE vself) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  db_lock(vself);
  bool rv = db->clear();
  db_unlock(vself);
  return rv ? Qtrue : Qfalse;
}


static VALUE db_synchronize(int argc, VALUE* argv, VALUE vself) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  VALUE vhard, vproc;
  rb_scan_args(argc, argv, "02", &vhard, &vproc);
  bool hard = vhard != Qnil && vhard != Qfalse;
  VALUE vrv;
  if (rb_respond_to(vproc, id_fproc_process)) {
    SoftFileProcessor proc(vproc);
    db_lock(vself);
    bool rv = db->synchronize(hard, &proc);
    db_unlock(vself);
    vrv = rv ? Qtrue : Qfalse;
  } else {
    db_lock(vself);
    bool rv = db->synchronize(hard, NULL);
    db_unlock(vself);
    vrv = rv ? Qtrue : Qfalse;
  }
  return vrv;
}


static VALUE db_begin_transaction(int argc, VALUE* argv, VALUE vself) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  VALUE vhard;
  rb_scan_args(argc, argv, "01", &vhard);
  bool hard = vhard != Qnil && vhard != Qfalse;
  bool err = false;
  while (true) {
    db_lock(vself);
    bool rv = db->begin_transaction_try(hard);
    db_unlock(vself);
    if (rv) break;
    if (db->error() != kc::PolyDB::Error::LOGIC) {
      err = true;
      break;
    }
    th_pass();
  }
  return err ? Qfalse : Qtrue;
}


static VALUE db_end_transaction(int argc, VALUE* argv, VALUE vself) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  VALUE vcommit;
  rb_scan_args(argc, argv, "01", &vcommit);
  bool commit = vcommit != Qfalse;
  db_lock(vself);
  bool rv = db->end_transaction(commit);
  db_unlock(vself);
  return rv ? Qtrue : Qfalse;
}


static VALUE db_count(VALUE vself) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  db_lock(vself);
  int64_t count = db->count();
  db_unlock(vself);
  return LL2NUM(count);
}


static VALUE db_size(VALUE vself) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  db_lock(vself);
  int64_t size = db->size();
  db_unlock(vself);
  return LL2NUM(size);
}


static VALUE db_path(VALUE vself) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  db_lock(vself);
  std::string path = db->path();
  db_unlock(vself);
  if (path.size() < 1) return Qnil;
  return rb_str_new2(path.c_str());
}


static VALUE db_status(VALUE vself) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  StringMap status;
  db_lock(vself);
  bool rv = db->status(&status);
  db_unlock(vself);
  return rv ? maptovhash(&status) : Qnil;
}


static VALUE db_cursor(VALUE vself) {
  return rb_class_new_instance(1, &vself, cls_cur);
}


static VALUE db_to_s(VALUE vself) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  db_lock(vself);
  std::string path = db->path();
  std::string str;
  kc::strprintf(&str, "#<KyotoCabinet::DB:%p: %s: %lld: %lld>",
                db, path.c_str(), (long long)db->count(), (long long)db->size());
  db_unlock(vself);
  return rb_str_new2(str.c_str());
}


static VALUE db_each(VALUE vself) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  SoftEachVisitor visitor;
  db_lock(vself);
  bool rv = db->iterate(&visitor, false);
  db_unlock(vself);
  return rv ? Qtrue : Qfalse;
}


static VALUE db_each_key(VALUE vself) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  SoftEachKeyVisitor visitor;
  db_lock(vself);
  bool rv = db->iterate(&visitor, false);
  db_unlock(vself);
  return rv ? Qtrue : Qfalse;
}


static VALUE db_each_value(VALUE vself) {
  kc::PolyDB* db;
  Data_Get_Struct(vself, kc::PolyDB, db);
  SoftEachValueVisitor visitor;
  db_lock(vself);
  bool rv = db->iterate(&visitor, false);
  db_unlock(vself);
  return rv ? Qtrue : Qfalse;
}


static VALUE db_process(int argc, VALUE* argv, VALUE vself) {
  VALUE vdb = rb_class_new_instance(0, NULL, cls_db);
  VALUE vpath, vmode;
  rb_scan_args(argc, argv, "02", &vpath, &vmode);
  VALUE vrv = rb_funcall(vdb, id_db_open, 2, vpath, vmode);
  if (vrv == Qnil || vrv == Qfalse) return rb_funcall(vdb, id_db_error, 0);
  VALUE bargs = rb_ary_new3(1, vdb);
  VALUE eargs = rb_ary_new3(1, vdb);
  return rb_ensure((METHOD)db_process_body, bargs, (METHOD)db_process_ensure, eargs);
}


static VALUE db_process_body(VALUE args) {
  VALUE vdb = rb_ary_shift(args);
  rb_yield(vdb);
  return Qnil;
}


static VALUE db_process_ensure(VALUE args) {
  VALUE vdb = rb_ary_shift(args);
  rb_funcall(vdb, id_db_close, 0);
  return Qnil;
}


}


// END OF FILE
