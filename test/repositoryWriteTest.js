var expect = require('expect.js'),
    async = require('async'),
    _ = require('lodash'),
    repository = require('../lib/repository'),
    ConcurrencyError = require('../lib/concurrencyError'),
    Base = require('../lib/base'),
    ViewModel = require('../lib/viewmodel'),
    InMemory = require('../lib/databases/inmemory');

function cleanRepo(repo, done) {
  repo.clearAll(done);
}

describe('Repository write', function() {

  describe('calling write', function() {

    describe('without options', function() {

      it('it should return with the in memory repository', function() {

        var repo = repository.write();
        expect(repo).to.be.a('object');

      });

      describe('but with a callback', function() {

        it('it should callback with repository object', function(done) {

          repository.write(function(err, repo) {
            expect(err).not.to.be.ok();
            expect(repo).to.be.a('object');
            done();
          });

        });

      });

    });

    describe('with options of a non existing db implementation', function() {

      it('it should throw an error', function() {

        expect(function() {
          repository.write({ type: 'strangeDb' });
        }).to.throwError();

      });

      it('it should callback with an error', function(done) {

        expect(function() {
          repository.write({ type: 'strangeDb' }, function(err) {
            expect(err).to.be.ok();
            done();
          });
        }).to.throwError();

      });

    });

    describe('with options of an own db implementation', function() {

      it('it should return with the an instance of that implementation', function() {

        var repo = repository.write({ type: InMemory });
        expect(repo).to.be.a(InMemory);

      });

    });

    describe('with options containing a type property with the value of', function() {

      var types = ['inmemory', 'mongodb', 'tingodb', 'redis'];

      types.forEach(function(type) {

        describe('"' + type + '"', function() {

          var repo;

          describe('without callback', function() {

            afterEach(function(done) {
              repo.disconnect(done);
            });

            it('it should not emit connect', function(done) {

              repo = repository.write({ type: type });
              repo.once('connect', function () {
                expect(true).to.eql(false);
              });

              setTimeout(done, 199);

            });

            it('it should return with the correct repository', function() {

              repo = repository.write({ type: type });
              expect(repo).to.be.a('object');
              expect(repo.connect).to.be.a('function');
              expect(repo.disconnect).to.be.a('function');
              expect(repo.getNewId).to.be.a('function');
              expect(repo.get).to.be.a('function');
              expect(repo.find).to.be.a('function');
              expect(repo.findOne).to.be.a('function');
              expect(repo.commit).to.be.a('function');
              expect(repo.checkConnection).to.be.a('function');
              expect(repo.extend).to.be.a('function');
              expect(repo.clear).to.be.a('function');

            });

            describe('calling connect', function () {

              it('it should emit connect', function(done) {

                repo = repository.write({ type: type });
                repo.once('connect', done);
                repo.connect();

              });

            });

          });

          describe('with callback', function() {

            afterEach(function(done) {
              repo.disconnect(done);
            });

            it('it should return with the correct repository', function(done) {

              repository.write({ type: type }, function(err, resR) {
                repo = resR;
                expect(resR).to.be.a('object');
                done();
              });

            });

          });

          describe('having connected', function() {

            describe('calling disconnect', function() {

              beforeEach(function(done) {
                repository.write({ type: type }, function(err, resR) {
                  repo = resR;
                  done();
                });
              });

              it('it should callback successfully', function(done) {

                repo.disconnect(function(err) {
                  expect(err).not.to.be.ok();
                  done();
                });

              });

              it('it should emit disconnect', function(done) {

                repo.once('disconnect', done);
                repo.disconnect();

              });

            });

            describe('using the repository', function() {

              var dummyRepo;

              before(function(done) {
                repository.write({ type: type }, function(err, resR) {
                  repo = resR;
                  dummyRepo = repo.extend({
                    collectionName: 'dummies'
                  });
                  done();
                });
              });

              beforeEach(function(done) {
                cleanRepo(dummyRepo, done);
              });

              describe('calling getNewId', function() {

                it('it should callback with a new Id as string', function(done) {

                  dummyRepo.getNewId(function(err, id) {
                    expect(err).not.to.be.ok();
                    expect(id).to.be.a('string');
                    done();
                  });

                });

              });

              describe('calling get', function() {

                describe('subscribing to before/after-database-get', function() {

                  it('it should emit the correct event', function(done) {

                    var receivedBefore = false;
                    var receivedAfter = false;
                    dummyRepo.on('before-database-get', function() { receivedBefore = true; });
                    dummyRepo.on('after-database-get', function() { receivedAfter = true; });

                    dummyRepo.get(function() {
                      expect(receivedBefore).to.eql(true);
                      expect(receivedAfter).to.eql(true);
                      done();
                    });

                  });

                });
                
                describe('without an id', function() {

                  it('it should return a new object with a new id', function(done) {

                    dummyRepo.get(function(err, obj) {
                      expect(obj).to.be.a(ViewModel);
                      expect(obj.id).to.be.ok();
                      done();
                    });

                  });

                });

                describe('with an id of a non-existing record', function() {

                  it('it should return a new object with the given id', function(done) {

                    dummyRepo.get('1234', function(err, obj) {
                      expect(obj.id).to.eql('1234');
                      done();
                    });

                  });

                  it('the returned object should have an actionOnCommit of create', function(done) {

                    dummyRepo.get('1234', function(err, obj) {
                      expect(obj).to.have.property('actionOnCommit', 'create');
                      done();
                    });

                  });

                });

                describe('with an id of an existing record', function() {

                  it('it should return a new object with the data of the record that matches the given id', function(done) {

                    dummyRepo.get('2345', function(err, obj) {
                      obj.set('foo', 'bar');
                      dummyRepo.commit(obj, function(err) {
                        dummyRepo.get(obj.id, function(err, obj2) {
                          expect(obj2.id).to.eql(obj.id);
                          expect(obj2.get('foo')).to.eql('bar');
                          done();
                        });
                      });
                    });

                  });

                  it('the returned object should have an actionOnCommit of update', function(done) {

                    dummyRepo.get('3456', function(err, obj) {
                      obj.set('foo', 'bar');
                      dummyRepo.commit(obj, function(err) {
                        dummyRepo.get(obj.id, function(err, obj2) {
                          expect(obj2).to.have.property('actionOnCommit', 'update');
                          done();
                        });
                      });
                    });

                  });

                });

              });

              describe('calling find', function() {

                describe('subscribing to before/after-database-find', function() {

                  it('it should emit the correct event', function(done) {

                    var receivedBefore = false;
                    var receivedAfter = false;
                    dummyRepo.on('before-database-find', function() { receivedBefore = true; });
                    dummyRepo.on('after-database-find', function() { receivedAfter = true; });

                    dummyRepo.find(function() {
                      expect(receivedBefore).to.eql(true);
                      expect(receivedAfter).to.eql(true);
                      done();
                    });

                  });

                });

                describe('without a query object', function() {

                  describe('having no records', function() {

                    it('it should return an empty array', function(done) {

                      dummyRepo.find(function(err, results) {
                        expect(results).to.be.an('array');
                        expect(results).to.have.length(0);
                        expect(results.toJSON).to.be.a('function');
                        done();
                      });

                    });

                  });

                  describe('having any records', function() {

                    beforeEach(function(done) {

                      dummyRepo.get('4567', function(err, vm) {
                        dummyRepo.commit(vm, function(err) {
                          dummyRepo.get('4568', function(err, vm) {
                            dummyRepo.commit(vm, done);
                          });
                        });
                      });

                    });

                    it('it should return all records within an array', function(done) {

                      dummyRepo.get('4567', function(err, vm1) {
                        dummyRepo.get('4568', function(err, vm2) {
                          dummyRepo.find(function(err, results) {
                            expect(results).to.have.length(2);
                            expect(results.toJSON).to.be.a('function');
                            expect(results[0].id === vm1.id || results[1].id === vm1.id);
                            expect(results[0].id === vm2.id || results[1].id === vm2.id);
                            done();
                          });
                        });
                      });

                    });

                    describe('calling toJSON on a result array', function() {

                      it('it should return the correct data', function (done) {

                        dummyRepo.get('4567', function(err, vm1) {
                          vm1.set('my', 'data');
                          vm1.commit(function(err) {
                            dummyRepo.get('4568', function(err, vm2) {
                              dummyRepo.find(function(err, results) {
                                var res = results.toJSON();
                                expect(res[0].id === vm1.id || res[1].id === vm1.id);
                                expect(res[0].id === vm2.id || res[1].id === vm2.id);
                                expect(res[0].my === 'data' || res[1].my === 'data');
                                done();
                              });
                            });
                          });
                        });

                      });

                    });

                    it('the containing objects should have an actionOnCommit property', function(done) {

                      dummyRepo.get('4567', function(err, vm1) {
                        dummyRepo.get('4568', function(err, vm2) {
                          dummyRepo.find(function(err, results) {
                            expect(results[0]).to.be.a(ViewModel);
                            expect(results[1]).to.be.a(ViewModel);
                            done();
                          });
                        });
                      });

                    });

                    it('the containing objects should have a set and a get and a destroy and a commit function', function(done) {

                      dummyRepo.get('4567', function(err, vm1) {
                        dummyRepo.get('4568', function(err, vm2) {
                          dummyRepo.find(function(err, results) {
                            expect(results[0]).to.be.a(ViewModel);
                            expect(results[1]).to.be.a(ViewModel);
                            done();
                          });
                        });
                      });

                    });

                  });

                });

                var limitedCompatabilityTypes = ['redis'];

                if (!_.includes(limitedCompatabilityTypes, type)) {

                  describe('with a query object', function() {

                    describe('having no records', function() {

                      it('it should return an empty array', function(done) {

                        dummyRepo.find({}, function(err, results) {
                          expect(results).to.be.an('array');
                          expect(results).to.have.length(0);
                          done();
                        });

                      });

                    });

                    describe('having any records', function() {

                      beforeEach(function(done) {

                        dummyRepo.get('4567', function(err, vm) {
                          vm.set('foo', 'bar');

                          dummyRepo.commit(vm, function(err) {
                            dummyRepo.get('4568', function(err, vm2) {

                              vm2.set('foo', 'wat');
                              dummyRepo.commit(vm2, done);
                            });
                          });
                        });

                      });

                      describe('not matching the query object', function() {

                        it('it should return an empty array', function(done) {

                          var query = { foo: 'bas' };

                          dummyRepo.find(query, function(err, results) {
                            expect(results).to.be.an('array');
                            expect(results).to.have.length(0);
                            done();
                          });

                        });

                      });

                      describe('matching the query object', function() {

                        it('it should return all matching records within an array', function(done) {

                          var query = { foo: 'bar' };

                          dummyRepo.find(query, function(err, results) {
                            expect(results).to.be.an('array');
                            expect(results).to.have.length(1);
                            done();
                          });

                        });

                      });

                      describe('matching the query object, that queries an array', function () {

                        beforeEach(function (done) {

                          dummyRepo.get('4567', function (err, vm) {
                            vm.set('foos', [
                              { foo: 'bar' }
                            ]);
                            dummyRepo.commit(vm, done);
                          });

                        });

                        it('it should return all matching records within an array', function (done) {

                          var query = { 'foos.foo': 'bar' };

                          dummyRepo.find(query, function (err, results) {
                            expect(results).to.be.an('array');
                            expect(results).to.have.length(1);
                            done();
                          });

                        });

                      });

                      var queryExtended = ['inmemory', 'mongodb', 'tingodb'];

                      if (_.includes(queryExtended, type)) {

                        describe('matching the query object,', function () {

                          beforeEach(function (done) {

                            dummyRepo.get('81234781', function (err, vm) {
                              vm.set('age', 18);
                              vm.set('name', 'abcdefg');
                              dummyRepo.commit(vm, function (err) {
                                dummyRepo.get('14123412', function (err, vm) {
                                  vm.set('age', 6);
                                  vm.set('special', true);
                                  vm.set('name', 'defghijklmnopq');
                                  dummyRepo.commit(vm, function (err) {
                                    dummyRepo.get('941931', function (err, vm) {
                                      vm.set('age', 34);
                                      vm.set('tags', ['a', 'b', 'c']);
                                      vm.set('name', 'opqrstuvwxyz');
                                      dummyRepo.commit(vm, done);
                                    });
                                  });
                                });
                              });
                            });

                          });

                          describe('that queries a range', function () {

                            it('it should return all matching records within an array', function (done) {

                            var query = {age: {$gte: 10, $lte: 20}};

                              dummyRepo.find(query, function (err, results) {
                                expect(results).to.be.an('array');
                                expect(results).to.have.length(1);
                                expect(results[0].get('age')).to.eql(18);
                                done();
                              });

                            });

                          });

                          describe('that queries with or', function () {

                            it('it should return all matching records within an array', function (done) {

                              var query = { $or: [{age: 18}, {special: true}] };

                              dummyRepo.find(query, function (err, results) {
                                expect(results).to.be.an('array');
                                expect(results).to.have.length(2);
                                done();
                              });
                            });

                          });

                          describe('that queries with and', function () {

                            it('it should return all matching records within an array', function (done) {

                              var query = { $and: [{age: 6}, {special: true}] };

                              dummyRepo.find(query, function (err, results) {
                                expect(results).to.be.an('array');
                                expect(results).to.have.length(1);
                                done();
                              });

                            });

                          });

                          describe('that queries and sorts 1/2', function () {

                            it('it should return all matching records within an array', function (done) {

                              var query = { $or: [{age: 18}, {special: true}] };
                              var queryOptions = { sort: [['age', 'desc']] };

                              dummyRepo.find(query, queryOptions, function (err, results) {
                                expect(results).to.be.an('array');
                                expect(results).to.have.length(2);
                                expect(results[0].get('age')).to.eql(18);
                                done();
                              });

                            });

                          });

                          describe('that queries and sorts 2/2', function () {

                            it('it should return all matching records within an array', function (done) {
                              var query = { $or: [{age: 18}, {special: true}] };
                              var queryOptions = { sort: { age: 1} };

                              dummyRepo.find(query, queryOptions, function (err, results) {
                                expect(results).to.be.an('array');
                                expect(results).to.have.length(2);
                                expect(results[0].get('age')).to.eql(6);
                                done();
                              });

                            });

                          });

                          describe('that queries with in', function () {

                            it('it should return all matching records within an array', function (done) {

                              var query = { age: { $in: [1, 2, 3, 6] } };

                              dummyRepo.find(query, function (err, results) {
                                expect(results).to.be.an('array');
                                expect(results).to.have.length(1);
                                expect(results[0].get('age')).to.eql(6);
                                done();
                              });

                            });

                          });

                          if (type !== 'tingodb') {

                            describe('that queries with regex', function () {

                              it('it should return all matching records within an array', function (done) {

                                var query = {$or:[{name:{$regex:'.*pq.*'}}]};
                                var queryOptions = { limit: 2, skip: 1, sort: { age: 1 } };

                                dummyRepo.find(query, queryOptions, function (err, results) {
                                  expect(results).to.be.an('array');
                                  expect(results).to.have.length(1);
                                  expect(results[0].get('age')).to.eql(34);
                                  done();
                                });

                              });

                            });

                          }

                          describe('with query options', function () {

                            describe('for paging limit: 2, skip: 1 and sort', function () {

                              it('it should work as expected', function (done) {

                                var query = { age: {$gte: 0} };
                                var queryOptions = { limit: 2, skip: 1, sort: { age: 1 } };

                                dummyRepo.find(query, queryOptions, function (err, results) {
                                  expect(results).to.be.an('array');
                                  expect(results).to.have.length(2);
                                  expect(results[0].get('age')).to.eql(18);
                                  expect(results[1].get('age')).to.eql(34);
                                  done();
                                });

                              });

                            });

                          });

                        });

                      }

                      var bulkCommit = ['inmemory', 'mongodb'];

                      if (_.includes(bulkCommit, type)) {

                        describe('bulkCommit,', function () {

                          describe('happy path', function() {

                            it('it should work as expected', function (done) {

                              dummyRepo.get('81234781_bulk', function (err, vm1) {
                                vm1.set('age', 18);
                                vm1.set('name', 'abcdefg');
                                vm1.set('bulk', true);
                                dummyRepo.get('14123412_bulk', function (err, vm2) {
                                  vm2.set('age', 6);
                                  vm2.set('special', true);
                                  vm2.set('name', 'defghijklmnopq');
                                  vm2.set('bulk', true);
                                  dummyRepo.get('941931_bulk', function (err, vm3) {
                                    vm3.set('age', 34);
                                    vm3.set('tags', ['a', 'b', 'c']);
                                    vm3.set('name', 'opqrstuvwxyz');
                                    vm3.set('bulk', true);

                                    // upsert
                                    var vm4 = new ViewModel({id: '941931_upsert_bulk', bulk: true }, dummyRepo);
                                    vm4.actionOnCommit = 'update'; // simulate create -> update -> update replay scenario
                                              
                                    
                                    dummyRepo.bulkCommit([vm1, vm2, vm3, vm4], function(err, vms) {
                                      expect(err).not.to.be.ok();
                                      expect(vms[0].actionOnCommit).to.eql('update');
                                      expect(vms[1].actionOnCommit).to.eql('update');
                                      expect(vms[2].actionOnCommit).to.eql('update');
                                      expect(vms[3].actionOnCommit).to.eql('update');
                                      expect(vms).to.have.length(4);

                                      var query = { bulk: true };
      
                                      dummyRepo.find(query, function (err, vms) {
                                        expect(err).not.to.be.ok();
                                        expect(vms).to.have.length(4);
  
                                        done();
                                      });
                                    });
                                  });
                                });
                              });
  
                            });

                          });

                          describe('concurrency on insert', function () {

                            it('it should work as expected', function (done) {
  
                              dummyRepo.get('81234781_bulk', function (err, vm1) {
                                vm1.set('age', 18);
                                vm1.set('name', 'abcdefg');
                                vm1.set('bulk', true);
                                dummyRepo.get('14123412_bulk', function (err, vm2) {
                                  vm2.set('age', 6);
                                  vm2.set('special', true);
                                  vm2.set('name', 'defghijklmnopq');
                                  vm2.set('bulk', true);
                                  dummyRepo.get('941931_bulk', function (err, vm3) {
                                    vm3.set('age', 34);
                                    vm3.set('tags', ['a', 'b', 'c']);
                                    vm3.set('name', 'opqrstuvwxyz');
                                    vm3.set('bulk', true);
  
                                    dummyRepo.get('941931_bulk', function (err, vm) {
                                      vm.set('age', 34);
                                      vm.set('tags', ['a', 'b', 'c']);
                                      vm.set('name', 'opqrstuvwxyz');
                                      vm.set('changedByOther', true);
                                      dummyRepo.commit(vm, function() {
                                        dummyRepo.bulkCommit([vm1, vm2, vm3], function(err) {
                                          expect(err).to.be.a(ConcurrencyError);
                                          done();
                                        });
                                      });
                                    });
                                  });
                                });
                              });
  
                            });
  
                          });

                          describe('concurrency on update', function () {

                            beforeEach(function(done) {
                              dummyRepo.get('941931_bulk', function (err, vm) {
                                vm.set('age', 34);
                                vm.set('tags', ['a', 'b', 'c']);
                                vm.set('name', 'opqrstuvwxyz');
                                vm.set('original', true);
                                dummyRepo.commit(vm, function() {
                                  dummyRepo.get('81234781_bulk', function (err, vm) {
                                    vm.set('age', 18);
                                    vm.set('name', 'abcdefg');
                                    vm.set('existing', true);
                                    vm.set('original', true);
                                    dummyRepo.commit(vm, done);
                                  });
                                });
                              });
                            });

                            it('it should work as expected', function (done) {

                              dummyRepo.get('81234781_bulk', function (err, vm1) {
                                vm1.set('age', 18);
                                vm1.set('name', 'abcdefg');
                                vm1.set('bulk', true);
                                dummyRepo.get('14123412_bulk', function (err, vm2) {
                                  vm2.set('age', 6);
                                  vm2.set('special', true);
                                  vm2.set('name', 'defghijklmnopq');
                                  vm2.set('bulk', true);
                                  dummyRepo.get('941931_bulk', function (err, vm3) {
                                    vm3.set('age', 34);
                                    vm3.set('tags', ['a', 'b', 'c']);
                                    vm3.set('name', 'opqrstuvwxyz');
                                    vm3.set('bulk', true);

                                    dummyRepo.get('941931_bulk', function (err, vm) {
                                      vm.set('age', 34);
                                      vm.set('tags', ['a', 'b', 'c']);
                                      vm.set('name', 'opqrstuvwxyz');
                                      vm.set('changedByOther', true);
                                      dummyRepo.commit(vm, function() {
                                        dummyRepo.bulkCommit([vm1, vm2, vm3], function(err) {
                                          expect(err).to.be.a(ConcurrencyError);
                                          done();
                                        });
                                      });
                                    });
                                  });
                                });
                              });
  
                            });
  
                          });

                          describe('concurrency on delete', function () {

                            beforeEach(function(done) {
                              dummyRepo.get('81234781_to_del_bulk', function (err, vm) {
                                vm.set('age', 18);
                                vm.set('name', 'abcdefg');
                                vm.set('existing', true);
                                vm.set('original', true);
                                dummyRepo.commit(vm, done);
                              });
                            });

                            it('it should work as expected', function (done) {
  
                              dummyRepo.get('81234781_to_del_bulk', function (err, vm1) {
                                vm1.destroy();
                                dummyRepo.get('941931_bulk', function (err, vm3) {
                                  vm3.set('age', 34);
                                  vm3.set('tags', ['a', 'b', 'c']);
                                  vm3.set('name', 'opqrstuvwxyz');
                                  vm3.set('bulk', true);

                                  dummyRepo.get('81234781_to_del_bulk', function (err, vm) {
                                    vm.set('changed', true);
                                    dummyRepo.commit(vm, function() {
                                      dummyRepo.bulkCommit([vm1, vm3], function(err) {
                                        expect(err).to.be.a(ConcurrencyError);
                                        done();
                                      });
                                    });
                                  });
                                });
                              });
  
                            });
  
                          });

                        });

                      }

                    });

                  });

                }

                describe('with query options', function () {

                  beforeEach(function(done) {

                    cleanRepo(dummyRepo, function() {
                      dummyRepo.get('4567', function(err, vm) {
                        vm.set('foo', 'bar');

                        dummyRepo.commit(vm, function(err) {
                          dummyRepo.get('4568', function(err, vm2) {

                            vm2.set('foo', 'wat');
                            dummyRepo.commit(vm2, function(err) {
                              dummyRepo.get('4569', function(err, vm3) {

                                vm3.set('foo', 'bit');
                                dummyRepo.commit(vm3, done);
                              });
                            });
                          });
                        });
                      });
                    });

                  });

                  describe('for paging limit: 2, skip: 1', function () {

                    it('it should work as expected', function (done) {

                      var queryOptions = { limit: 2, skip: 1 };

                      dummyRepo.find({}, queryOptions, function (err, results) {
                        expect(results).to.be.an('array');
                        expect(results.length).to.eql(2);
                        expect(results.toJSON).to.be.a('function');
                        expect(results[0].get('foo') === 'wat' || results[1].get('foo') === 'wat');
                        expect(results[0].get('foo') === 'bit' || results[1].get('foo') === 'bit');

                        done();
                      });

                    });

                  });

                });

              });

              describe('calling findOne', function() {

                describe('subscribing to before/after-database-findOne', function() {

                  it('it should emit the correct event', function(done) {

                    var receivedBefore = false;
                    var receivedAfter = false;
                    dummyRepo.on('before-database-findOne', function() { receivedBefore = true; });
                    dummyRepo.on('after-database-findOne', function() { receivedAfter = true; });

                    dummyRepo.findOne(function() {
                      expect(receivedBefore).to.eql(true);
                      expect(receivedAfter).to.eql(true);
                      done();
                    });

                  });

                });

                describe('without a query object', function() {

                  describe('having no records', function() {

                    it('it should return an empty array', function(done) {

                      dummyRepo.findOne(function(err, result) {
                        expect(result).not.to.be.ok();
                        done();
                      });

                    });

                  });

                  describe('having any records', function() {

                    beforeEach(function(done) {

                      dummyRepo.get('4567', function(err, vm) {
                        dummyRepo.commit(vm, function(err) {
                          dummyRepo.get('4568', function(err, vm) {
                            dummyRepo.commit(vm, done);
                          });
                        });
                      });

                    });

                    it('it should return all records within an array', function(done) {

                      dummyRepo.get('4567', function(err, vm1) {
                        dummyRepo.get('4568', function(err, vm2) {
                          dummyRepo.findOne(function(err, result) {
                            expect(result.id === vm1.id);
                            done();
                          });
                        });
                      });

                    });

                    describe('calling toJSON on a result array', function() {

                      it('it should return the correct data', function (done) {

                        dummyRepo.get('4567', function(err, vm1) {
                          vm1.set('my', 'data');
                          vm1.commit(function(err) {
                            dummyRepo.get('4568', function(err, vm2) {
                              dummyRepo.findOne(function(err, result) {
                                var res = result.toJSON();
                                expect(res.id === vm1.id);
                                expect(res.id === vm2.id);
                                expect(res.my === 'data');
                                done();
                              });
                            });
                          });
                        });

                      });

                    });

                    it('the containing objects should have an actionOnCommit property', function(done) {

                      dummyRepo.get('4567', function(err, vm1) {
                        dummyRepo.get('4568', function(err, vm2) {
                          dummyRepo.findOne(function(err, result) {
                            expect(result).to.be.a(ViewModel);
                            done();
                          });
                        });
                      });

                    });

                    it('the containing objects should have a set and a get and a destroy and a commit function', function(done) {

                      dummyRepo.get('4567', function(err, vm1) {
                        dummyRepo.get('4568', function(err, vm2) {
                          dummyRepo.findOne(function(err, result) {
                            expect(result).to.be.a(ViewModel);
                            done();
                          });
                        });
                      });

                    });

                  });

                });

                var limitedCompatabilityTypes = ['redis'];

                if (!_.includes(limitedCompatabilityTypes, type)) {

                  describe('with a query object', function() {

                    describe('having no records', function() {

                      it('it should return an empty array', function(done) {

                        dummyRepo.findOne({}, function(err, result) {
                          expect(result).not.to.be.ok();
                          done();
                        });

                      });

                    });

                    describe('having any records', function() {

                      beforeEach(function(done) {

                        dummyRepo.get('4567', function(err, vm) {
                          vm.set('foo', 'bar');

                          dummyRepo.commit(vm, function(err) {
                            dummyRepo.get('4568', function(err, vm2) {

                              vm2.set('foo', 'wat');
                              dummyRepo.commit(vm2, done);
                            });
                          });
                        });

                      });

                      describe('not matching the query object', function() {

                        it('it should return an empty array', function(done) {
                          var query = { 'foo': 'bas' };

                          dummyRepo.findOne(query, function(err, result) {
                            expect(result).not.to.be.ok();
                            done();
                          });

                        });

                      });

                      describe('matching the query object', function() {

                        it('it should return all matching records within an array', function(done) {

                          var query = { 'foo': 'bar' };

                          dummyRepo.findOne(query, function(err, result) {
                            expect(result).to.be.ok();
                            done();
                          });

                        });

                      });

                      describe('matching the query object, that queries an array', function () {

                        beforeEach(function (done) {

                          dummyRepo.get('4567', function (err, vm) {
                            vm.set('foos', [
                              { foo: 'bar' }
                            ]);
                            dummyRepo.commit(vm, done);
                          });

                        });

                        it('it should return all matching records within an array', function (done) {

                          var query = { 'foos.foo': 'bar' };

                          dummyRepo.findOne(query, function (err, result) {
                            expect(result).to.be.ok();
                            done();
                          });

                        });

                      });

                    });

                  });

                }

              });

              describe('calling commit', function() {

                describe('subscribing to before/after-database-commit', function() {

                  it('it should emit the correct event', function(done) {

                    var receivedBefore = false;
                    var receivedAfter = false;
                    dummyRepo.on('before-database-commit', function() { receivedBefore = true; });
                    dummyRepo.on('after-database-commit', function() { receivedAfter = true; });

                    var obj = {
                      foo: 'barrrr',
                      set: function (k, v) {
                        key = k;
                        value = v;
                      }
                    };

                    dummyRepo.commit(obj, function() {
                      expect(receivedBefore).to.eql(true);
                      expect(receivedAfter).to.eql(true);
                      done();
                    });

                  });

                });

                describe('with a single object', function() {

                  describe('not having an actionOnCommit', function() {

                    it('it should not modify the view model database', function(done) {

                      var key, value;
                      var obj = {
                        foo: 'bar',
                        set: function (k, v) {
                          key = k;
                          value = v;
                        }
                      };

                      dummyRepo.commit(obj, function(err) {
                        expect(key).to.eql('commitStamp');
                        expect(value).to.be.a('number');
                        dummyRepo.find(function(err, results) {
                          expect(results).to.be.an('array');
                          expect(results).to.have.length(0);
                          done();
                        });
                      });

                    });

                    it('it should callback with error', function(done) {

                      var key, value;
                      var obj = {
                        foo: 'bar',
                        set: function (k, v) {
                          key = k;
                          value = v;
                        }
                      };

                      dummyRepo.commit(obj, function(err) {
                        expect(key).to.eql('commitStamp');
                        expect(value).to.be.a('number');
                        expect(err).to.be.ok();
                        done();
                      });

                    });

                  });

                  describe('having an invalid actionOnCommit', function() {

                    it('it should not modify the view model database', function(done) {

                      var key, value;
                      var obj = {
                        actionOnCommit: 'nufta',
                        foo: 'bar',
                        set: function (k, v) {
                          key = k;
                          value = v;
                        }
                      };

                      dummyRepo.commit(obj, function(err) {
                        expect(key).to.eql('commitStamp');
                        expect(value).to.be.a('number');
                        dummyRepo.find(function(err, results) {
                          expect(results).to.be.an('array');
                          expect(results).to.have.length(0);
                          done();
                        });
                      });

                    });

                    it('it should callback with error', function(done) {

                      var key, value;
                      var obj = {
                        actionOnCommit: 'nufta',
                        foo: 'bar',
                        set: function (k, v) {
                          key = k;
                          value = v;
                        }
                      };

                      dummyRepo.commit(obj, function(err) {
                        expect(key).to.eql('commitStamp');
                        expect(value).to.be.a('number');
                        expect(err).to.be.ok();
                        done();
                      });

                    });

                  });

                  describe('having an actionOnCommit', function() {

                    beforeEach(function(done) {

                      dummyRepo.get('4567', function(err, vm) {
                        vm.set('foo', 'bar');

                        dummyRepo.commit(vm, function(err) {
                          dummyRepo.get('4568', function(err, vm2) {

                            vm2.set('foo', 'wat');
                            dummyRepo.commit(vm2, done);
                          });
                        });
                      });

                    });

                    describe('of create', function() {

                      describe('on a non-existing record', function() {

                        var obj, retObj;

                        it('it should insert a new record', function(done) {

                          dummyRepo.get('4569', function(err, vm) {
                            vm.set('foo', 'baz');
                            obj = vm;
                            dummyRepo.commit(vm, function(err, ret) {
                              retObj = ret;
                              dummyRepo.get('4569', function(err, vm2) {
                                vm.actionOnCommit = 'update';
                                expect(vm2.id).to.eql(vm.id);
                                expect(vm2.foo).to.eql(vm.foo);
                                done();
                              });
                            });
                          });

                        });

                        describe('and', function() {

                          it('it should return the vm with updated actionOnCommit', function() {

                            expect(obj.id).to.eql(retObj.id);
                            expect(retObj.actionOnCommit).to.eql('update');

                          });

                        });

                      });

                    });

                    describe('of update', function() {

                      describe('on a non-existing record', function() {

                        it('it should insert a new record', function(done) {

                          dummyRepo.get('4569', function(err, vm) {
                            vm.actionOnCommit = 'update';
                            vm.set('foo', 'baz');
                            dummyRepo.commit(vm, function(err) {
                              dummyRepo.get('4569', function(err, vm2) {
                                expect(vm2.id).to.eql(vm.id);
                                expect(vm2.get('foo')).to.eql(vm.get('foo'));
                                done();
                              });
                            });
                          });

                        });

                      });

                      describe('on an existing record', function() {

                        var obj, retObj;

                        it('it should update the existing record', function(done) {

                          dummyRepo.get('4567', function(err, vm) {
                            vm.set('foo', 'baz');
                            obj = vm;
                            dummyRepo.commit(vm, function(err, ret) {
                              retObj = ret;
                              dummyRepo.get('4567', function(err, vm2) {
                                expect(vm2.id).to.eql(vm.id);
                                expect(vm2.get('foo')).to.eql(vm.get('foo'));
                                done();
                              });
                            });
                          });

                        });

                        describe('and', function() {

                          it('it should return the vm with updated actionOnCommit', function() {

                            expect(obj.id).to.eql(retObj.id);
                            expect(retObj.actionOnCommit).to.eql('update');

                          });

                        });

                        describe('but beeing updated by someone else in the meantime', function() {

                          it('it should callback with a concurrency error', function(done) {

                            dummyRepo.get('456789123', function(err, vm) {
                              vm.set('foo', 'baz');
                              dummyRepo.commit(vm, function(err, ret) {
                                dummyRepo.get('456789123', function(err, vm2) {
                                  vm2.set('foo', 'baz2');
                                  dummyRepo.commit(vm2, function(err, ret) {
                                    dummyRepo.get('456789123', function(err, vm3) {
                                      vm.set('foo', 'blablalba');
                                      dummyRepo.commit(vm, function(err, ret) {
                                        expect(err).to.be.a(ConcurrencyError);
                                        done();
                                      });
                                    });
                                  });
                                });
                              });
                            });

                          });

                        });

                        describe('but beeing updated by someone else in the meantime and creating with the same id', function() {

                          it('it should callback with a concurrency error', function(done) {

                            dummyRepo.get('6677558899', function(err, vm) {
                              vm.set('foo', 'baz');
                              dummyRepo.get('6677558899', function(err, vm2) {
                                vm2.set('foo2', 'bag');
                                dummyRepo.commit(vm, function(err, ret) {
                                  dummyRepo.commit(vm2, function(err, ret) {
                                    expect(err).to.be.a(ConcurrencyError);
                                    done();
                                  });
                                });
                              });
                            });

                          });

                        });

                      });

                    });

                    describe('of delete', function() {

                      describe('on a non-existing record', function() {

                        it('it should not modify the view model database', function(done) {

                          dummyRepo.get('4567123', function(err, vm) {
                            vm.id = '4569123';
                            vm.destroy();

                            dummyRepo.commit(vm, function(err) {
                              expect(err).not.to.be.ok();

                              dummyRepo.find(function(err, results) {
                                expect(results).to.be.an('array');
                                expect(results).to.have.length(2);
                                done();
                              });
                            });
                          });

                        });

                      });

                      describe('on an existing record', function() {

                        it('it should delete the existing record', function(done) {

                          dummyRepo.get('4567', function(err, vm) {
                            vm.destroy();

                            dummyRepo.commit(vm, function(err) {
                              expect(err).not.to.be.ok();
                              dummyRepo.find(function(err, results) {
                                expect(results).to.be.an('array');
                                expect(results).to.have.length(1);
                                done();
                              });
                            });
                          });

                        });

                        describe('but beeing updated by someone else in the meantime', function() {

                          it('it should callback with a concurrency error', function(done) {

                            var org = new ViewModel({id: '34567891232'}, dummyRepo);
                            dummyRepo.get('34567891232', function(err, vm) {
                              vm.set('foo', 'baz');
                              dummyRepo.commit(vm, function(err, ret) {
                                dummyRepo.get('34567891232', function(err, vm2) {
                                  vm2.set('foo', 'baz2');
                                  org.set(vm2.toJSON());
                                  org.version = vm2.version;
                                  dummyRepo.commit(vm2, function(err, ret) {
                                    dummyRepo.get('34567891232', function(err, vm3) {
                                      org.destroy();
                                      dummyRepo.commit(org, function(err, ret) {
                                        expect(err).to.be.a(ConcurrencyError);
                                        done();
                                      });
                                    });
                                  });
                                });
                              });
                            });

                          });

                        });

                      });

                    });

                  });

                });

                describe('on a single object', function() {

                  describe('having an actionOnCommit', function() {

                    beforeEach(function(done) {

                      dummyRepo.get('4567', function(err, vm) {
                        vm.set('foo', 'bar');

                        dummyRepo.commit(vm, function(err) {
                          dummyRepo.get('4568', function(err, vm2) {

                            vm2.set('foo', 'wat');
                            dummyRepo.commit(vm2, done);
                          });
                        });
                      });

                    });

                    describe('of create', function() {

                      describe('on a non-existing record', function() {

                        it('it should insert a new record', function(done) {

                          dummyRepo.get('4569', function(err, vm) {
                            vm.set('foo', 'baz');
                            vm.commit(function(err) {
                              dummyRepo.get('4569', function(err, vm2) {
                                vm.actionOnCommit = 'update';
                                expect(vm2.id).to.eql(vm.id);
                                expect(vm2.get('foo')).to.eql(vm.get('foo'));
                                done();
                              });
                            });
                          });

                        });

                      });

                    });

                    describe('of update', function() {

                      describe('on a non-existing record', function() {

                        it('it should insert a new record', function(done) {

                          dummyRepo.get('4569', function(err, vm) {
                            vm.actionOnCommit = 'update';
                            vm.set('foo', 'baz');
                            vm.commit(function(err) {
                              dummyRepo.get('4569', function(err, vm2) {
                                expect(vm2.id).to.eql(vm.id);
                                expect(vm2.get('foo')).to.eql(vm.get('foo'));
                                done();
                              });
                            });
                          });

                        });

                      });

                      describe('on an existing record', function() {

                        it('it should update the existing record', function(done) {

                          dummyRepo.get('4567', function(err, vm) {
                            vm.set('foo', 'baz');
                            vm.commit(function(err) {
                              dummyRepo.get('4567', function(err, vm2) {
                                expect(vm2.id).to.eql(vm.id);
                                expect(vm2.get('foo')).to.eql(vm.get('foo'));
                                done();
                              });
                            });
                          });

                        });

                      });

                    });

                    describe('of delete', function() {

                      describe('on a non-existing record', function() {

                        it('it should not modify the view model database', function(done) {

                          dummyRepo.get('4567123', function(err, vm) {
                            vm.id = '4569123';
                            vm.destroy();

                            vm.commit(function(err) {
                              dummyRepo.find(function(err, results) {
                                expect(results).to.be.an('array');
                                expect(results).to.have.length(2);
                                done();
                              });
                            });
                          });

                        });

                      });

                      describe('on an existing record', function() {

                        it('it should delete the existing record', function(done) {

                          dummyRepo.get('4567', function(err, vm) {
                            vm.destroy();

                            vm.commit(function(err) {
                              dummyRepo.find(function(err, results) {
                                expect(results).to.be.an('array');
                                expect(results).to.have.length(1);
                                done();
                              });
                            });
                          });

                        });

                      });

                    });

                  });

                });

              });

              describe('calling clear', function() {

                beforeEach(function(done) {

                  dummyRepo.get('hahaha1', function(err, vm) {
                    dummyRepo.commit(vm, function(err) {
                      dummyRepo.get('hahaha2', function(err, vm) {
                        dummyRepo.commit(vm, done);
                      });
                    });
                  });

                });

                it('it should work as expected', function(done) {

                  expect(function() {
                    dummyRepo.clear(function (err) {
                      expect(err).not.to.be.ok();
                      dummyRepo.find(function (err, vms) {
                        expect(err).not.to.be.ok();
                        expect(vms.length).to.eql(0);
                        done();
                      });
                    });
                  }).not.to.throwError();

                });

              });

            });

          });

        });

      });

    });

	});

});
